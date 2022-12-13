"""Database interface for persisted scan data."""

import dataclasses as dc
import uuid
from typing import Any, AsyncIterator, Type, TypeVar

from dacite import from_dict  # type: ignore[attr-defined]
from motor.motor_asyncio import (  # type: ignore
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
)
from pymongo import ReturnDocument
from tornado import web

from ..config import LOGGER
from . import schema

# -----------------------------------------------------------------------------


_DB_NAME = "SkyDriver_DB"
_MANIFEST_COLL_NAME = "Manifests"
_RESULTS_COLL_NAME = "Results"
S = TypeVar("S", bound=schema.ScanIDDataclass)


async def ensure_indexes(motor_client: AsyncIOMotorClient) -> None:
    """Create indexes in collections.

    Call on server startup.
    """

    async def index_collection(coll: str, indexes: dict[str, bool]) -> None:
        for index, unique in indexes.items():
            await motor_client[_DB_NAME][coll].create_index(
                index, name=f"{index}_index", unique=unique
            )
        # LOGGER.debug(motor_client[_DB_NAME][coll].list_indexes())

    await index_collection(_MANIFEST_COLL_NAME, {"scan_id": True})
    await index_collection(_RESULTS_COLL_NAME, {"scan_id": True})


async def drop_collections(motor_client: AsyncIOMotorClient) -> None:
    """Drop the "regular" collections -- most useful for testing."""
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].drop()
    await motor_client[_DB_NAME][_RESULTS_COLL_NAME].drop()


class ScanIDCollectionFacade:
    """Allows specific semantic actions on the collections by Scan ID."""

    def __init__(self, motor_client: AsyncIOMotorClient) -> None:
        # place in a dictionary so there's some safeguarding against bogus collections
        self._collections: dict[str, AsyncIOMotorCollection] = {
            _MANIFEST_COLL_NAME: motor_client[_DB_NAME][_MANIFEST_COLL_NAME],
            _RESULTS_COLL_NAME: motor_client[_DB_NAME][_RESULTS_COLL_NAME],
        }

    async def _find_one(
        self, coll: str, scan_id: str, scandc_type: Type[S], incl_del: bool
    ) -> S:
        """Get document by 'scan_id'."""
        LOGGER.debug(f"finding: ({coll=}) doc with {scan_id=} for {scandc_type=}")
        query = {"scan_id": scan_id}
        doc = await self._collections[coll].find_one(query)
        if (not doc) or (doc["is_deleted"] and not incl_del):
            raise web.HTTPError(
                404,
                log_message=f"Document Not Found: {coll} document ({scan_id})",
            )
        scandc = from_dict(scandc_type, doc)
        LOGGER.debug(f"found: ({coll=}) doc {scandc}")
        return scandc  # type: ignore[no-any-return]  # mypy internal bug

    async def _upsert(self, coll: str, scandc: S) -> S:
        """Insert/update the doc."""
        LOGGER.debug(f"replacing: ({coll=}) doc with {scandc=}")
        doc = await self._collections[coll].find_one_and_replace(
            {"scan_id": scandc.scan_id},
            dc.asdict(scandc),
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if not doc:
            raise web.HTTPError(
                500,
                log_message=f"Failed to insert/update {coll} document ({scandc.scan_id})",
            )
        scandc = from_dict(type(scandc), doc)
        LOGGER.debug(f"replaced: ({coll=}) doc {scandc}")
        return scandc


# -----------------------------------------------------------------------------


class ManifestClient(ScanIDCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def find_one(self, scan_id: str, incl_del: bool) -> schema.Manifest:
        """Find one manifest and return it."""
        return await self._find_one(
            _MANIFEST_COLL_NAME, scan_id, schema.Manifest, incl_del
        )

    async def upsert(self, scandc: schema.Manifest) -> schema.Manifest:
        """Insert/update manifest and return it."""
        return await self._upsert(_MANIFEST_COLL_NAME, scandc)

    # ------------------------------------------------------------------

    async def get(self, scan_id: str, incl_del: bool) -> schema.Manifest:
        """Get `schema.Manifest` using `scan_id`."""
        LOGGER.debug(f"getting manifest for {scan_id=}")
        manifest = await self.find_one(scan_id, incl_del)
        return manifest

    async def post(self, event_id: str) -> schema.Manifest:
        """Create `schema.Manifest` doc."""
        LOGGER.debug(f"creating manifest for {event_id=}")
        manifest = schema.Manifest(
            uuid.uuid4().hex,
            False,
            event_id,
            # TODO: more args here
        )
        manifest = await self.upsert(manifest)
        return manifest

    async def patch(self, scan_id: str, progress: dict[str, Any]) -> schema.Manifest:
        """Update `progress` at doc matching `scan_id`."""
        LOGGER.debug(f"patching progress for {scan_id=}")
        if not progress:
            msg = f"Attempted progress update with an empty object ({progress}) for {scan_id=}"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        manifest = await self.find_one(scan_id, True)
        manifest.progress = progress
        manifest = await self.upsert(manifest)
        return manifest

    async def mark_as_deleted(self, scan_id: str) -> schema.Manifest:
        """Mark `schema.Manifest` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking manifest as deleted for {scan_id=}")
        manifest = await self.find_one(scan_id, True)
        manifest.is_deleted = True
        manifest = await self.upsert(manifest)
        return manifest

    async def get_scan_ids(self, event_id: str, incl_del: bool) -> AsyncIterator[str]:
        """Search over scans and find all matching event-id."""
        LOGGER.debug(f"matching: scan ids for {event_id=} ({incl_del=})")

        # skip the dataclass-casting b/c we're just returning a str
        query = {"event_id": event_id}
        async for doc in self._collections[_MANIFEST_COLL_NAME].find(query):
            if not incl_del and doc["is_deleted"]:
                continue
            LOGGER.debug(f"match: {doc['scan_id']=} for {event_id=} ({incl_del=})")
            yield doc["scan_id"]


# -----------------------------------------------------------------------------


class ResultClient(ScanIDCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def find_one(self, scan_id: str, incl_del: bool) -> schema.Result:
        """Find one result and return it."""
        return await self._find_one(
            _RESULTS_COLL_NAME, scan_id, schema.Result, incl_del
        )

    async def upsert(self, scandc: schema.Result) -> schema.Result:
        """Insert/update result and return it."""
        return await self._upsert(_RESULTS_COLL_NAME, scandc)

    # ------------------------------------------------------------------

    async def get(self, scan_id: str, incl_del: bool) -> schema.Result | None:
        """Get `schema.Result` using `scan_id`."""
        LOGGER.debug(f"getting result for {scan_id=}")
        result = await self.find_one(scan_id, incl_del)
        return result

    async def put(self, scan_id: str, json_dict: dict[str, Any]) -> schema.Result:
        """Override `schema.Result` at doc matching `scan_id`."""
        LOGGER.debug(f"overriding result for {scan_id=}")
        if not json_dict:
            msg = f"Attempted to add result with an empty object ({json_dict})"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        result = schema.Result(scan_id, False, json_dict)
        result = await self.upsert(result)
        return result

    async def mark_as_deleted(self, scan_id: str) -> schema.Result:
        """Mark `schema.Result` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking result as deleted for {scan_id=}")
        result = await self.find_one(scan_id, True)
        result.is_deleted = True
        result = await self.upsert(result)
        return result
