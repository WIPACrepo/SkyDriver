"""Database interface for persisted scan data."""

import dataclasses as dc
import logging
import uuid
from typing import Any, AsyncIterator, Dict, Type, TypeVar

# import pymongo.errors
from dacite import from_dict  # type: ignore[attr-defined]
from motor.motor_tornado import MotorClient, MotorCollection  # type: ignore
from tornado import web


@dc.dataclass(frozen=True)
class Progress:
    """Encompasses the computational progress of a scan."""


@dc.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str
    is_deleted: bool


@dc.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    json: dict


@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_id: str
    progress: Progress = Progress()


# -----------------------------------------------------------------------------


class DocumentNotFoundError(Exception):
    """Raised when a document is not found."""

    def __init__(self, collection: str, query: Dict[str, Any]):
        super().__init__(f"{collection}: {query}")


_DB_NAME = "SkyDriver_DB"
_MANIFEST_COLL_NAME = "Manifests"
_RESULTS_COLL_NAME = "Results"
S = TypeVar("S", bound=ScanIDDataclass)


async def ensure_indexes(motor_client: MotorClient) -> None:
    """Create indexes in collections.

    Call on server startup.
    """

    async def index_collection(coll: str, indexes: Dict[str, bool]) -> None:
        for index, unique in indexes.items():
            await motor_client[_DB_NAME][coll].create_index(
                index, name=f"{index}_index", unique=unique
            )
        # logging.debug(motor_client[_DB_NAME][coll].list_indexes())

    await index_collection(_MANIFEST_COLL_NAME, {"scan_id": True})
    await index_collection(_RESULTS_COLL_NAME, {"scan_id": True})


class ScanIDCollectionFacade:
    """Allows specific semantic actions on the collections by Scan ID."""

    def __init__(self, motor_client: MotorClient) -> None:
        # place in a dictionary so there's some safeguarding against bogus collections
        self._collections: Dict[str, MotorCollection] = {
            _MANIFEST_COLL_NAME: motor_client[_DB_NAME][_MANIFEST_COLL_NAME],
            _RESULTS_COLL_NAME: motor_client[_DB_NAME][_RESULTS_COLL_NAME],
        }

    async def _find_one(self, coll: str, scan_id: str, scandc_type: Type[S]) -> S:
        """Get document by 'scan_id'."""
        logging.debug(f"in {coll=} finding doc with {scan_id=} for {scandc_type=}")
        query = {"scan_id": scan_id}
        doc = await self._collections[coll].find_one(query)

        if not doc:
            raise DocumentNotFoundError(coll, query)
        return from_dict(scandc_type, doc)  # type: ignore[no-any-return] # mypy's erring

    async def _upsert(self, coll: str, scandc: S) -> S:
        """Insert/update the doc."""
        logging.debug(f"in {coll=} replacing doc with {scandc=}")
        res = await self._collections[coll].replace_one(
            {"scan_id": scandc.scan_id},
            dc.asdict(scandc),
            upsert=True,
        )
        if not res.modified_count:
            raise web.HTTPError(
                400,  # TODO - change to 500
                reason=f"Failed to insert {coll} document ({scandc.scan_id})",
            )
        return from_dict(type(scandc), res.raw_result)  # type: ignore[no-any-return] # mypy's erring


# -----------------------------------------------------------------------------


class ManifestClient(ScanIDCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def find_one(self, scan_id: str) -> Manifest:
        """Find one manifest and return it."""
        return await self._find_one(_MANIFEST_COLL_NAME, scan_id, Manifest)

    async def upsert(self, scandc: Manifest) -> Manifest:
        """Insert/update manifest and return it."""
        return await self._upsert(_MANIFEST_COLL_NAME, scandc)

    # ------------------------------------------------------------------

    async def get(self, scan_id: str) -> Manifest:
        """Get `Manifest` using `scan_id`."""
        logging.debug(f"getting manifest for {scan_id=}")
        manifest = await self.find_one(scan_id)
        return manifest

    async def post(self, event_id: str) -> Manifest:
        """Create `Manifest` doc."""
        logging.debug(f"creating manifest for {event_id=}")
        manifest = Manifest(
            uuid.uuid4().hex,
            False,
            event_id,
            # TODO: more args here
        )
        manifest = await self.upsert(manifest)
        return manifest

    async def patch(self, scan_id: str, progress: Progress) -> Manifest:
        """Update `progress` at doc matching `scan_id`."""
        logging.debug(f"patching progress for {scan_id=}")
        manifest = await self.find_one(scan_id)
        manifest.progress = progress
        manifest = await self.upsert(manifest)
        return manifest

    async def mark_as_deleted(self, scan_id: str) -> Manifest:
        """Mark `Manifest` at doc matching `scan_id` as deleted."""
        logging.debug(f"marking manifest as deleted for {scan_id=}")
        manifest = await self.find_one(scan_id)
        manifest.is_deleted = True
        manifest = await self.upsert(manifest)
        return manifest

    async def get_scan_ids(self, event_id: str, incl_del: bool) -> AsyncIterator[str]:
        """Search over scans and find all matching event-id."""
        logging.debug(f"get matching scan ids for {event_id=} ({incl_del=})")

        # skip the dataclass-casting b/c we're just returning a str
        query = {"event_id": event_id}
        async for doc in self._collections[_MANIFEST_COLL_NAME].find(query):
            if not incl_del and doc["is_deleted"]:
                continue
            yield doc["scan_id"]


# -----------------------------------------------------------------------------


class ResultClient(ScanIDCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def find_one(self, scan_id: str) -> Result:
        """Find one result and return it."""
        return await self._find_one(_RESULTS_COLL_NAME, scan_id, Result)

    async def upsert(self, scandc: Result) -> Result:
        """Insert/update result and return it."""
        return await self._upsert(_RESULTS_COLL_NAME, scandc)

    # ------------------------------------------------------------------

    async def get(self, scan_id: str) -> Result | None:
        """Get `Result` using `scan_id`."""
        logging.debug(f"getting result for {scan_id=}")
        try:
            result = await self.find_one(scan_id)
        except DocumentNotFoundError:
            return None
        return result

    async def put(self, scan_id: str, json_result: dict) -> Result:
        """Override `Result` at doc matching `scan_id`."""
        logging.debug(f"overriding result for {scan_id=}")
        result = Result(scan_id, False, json_result)
        result = await self.upsert(result)
        return result

    async def mark_as_deleted(self, scan_id: str) -> Result:
        """Mark `Result` at doc matching `scan_id` as deleted."""
        logging.debug(f"marking result as deleted for {scan_id=}")
        result = await self.find_one(scan_id)
        result.is_deleted = True
        result = await self.upsert(result)
        return result
