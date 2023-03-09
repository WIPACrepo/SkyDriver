"""Database interface for persisted scan data."""

import dataclasses as dc
from typing import Any, AsyncIterator, Type, TypeVar

from dacite import from_dict  # type: ignore[attr-defined]
from motor.motor_asyncio import (  # type: ignore
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
)
from pymongo import DESCENDING, ReturnDocument
from tornado import web
from typeguard import check_type

from ..config import LOGGER
from . import schema


def friendly_nested_asdict(value: Any) -> Any:
    """Convert any founded nested dataclass to dict if applicable.

    Like `dc.asdict()` but safe for any type and list- and dict-
    friendly.
    """
    if isinstance(value, dict):
        return {k: friendly_nested_asdict(v) for k, v in value.items()}

    if isinstance(value, list):
        return [friendly_nested_asdict(v) for v in value]

    if not dc.is_dataclass(value):
        return value

    return dc.asdict(value)


# -----------------------------------------------------------------------------


_DB_NAME = "SkyDriver_DB"
_MANIFEST_COLL_NAME = "Manifests"
_RESULTS_COLL_NAME = "Results"
S = TypeVar("S", bound=schema.ScanIDDataclass)


async def ensure_indexes(motor_client: AsyncIOMotorClient) -> None:
    """Create indexes in collections.

    Call on server startup.
    """
    # MANIFEST COLL
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].create_index(
        "scan_id",
        name="scan_id_index",
        unique=True,
    )
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].create_index(
        [
            ("event_metadata.event_id", DESCENDING),
            ("event_metadata.run_id", DESCENDING),
        ],
        name="event_run_index",
    )

    # RESULTS COLL
    await motor_client[_DB_NAME][_RESULTS_COLL_NAME].create_index(
        "scan_id",
        name="scan_id_index",
        unique=True,
    )


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

    async def _upsert(
        self,
        coll: str,
        scan_id: str,
        update: schema.StrDict | S,
        dclass: Type[S] | None = None,
    ) -> S:
        """Insert/update the doc.

        *For partial updates:* pass `update` as a `dict` along with a
        `scandc_type` (`ScanIDDataclass` class/type). `scandc_type` is
        used to validate updates against the document's schema and casts
        the returned document.

        *For whole inserts:* pass `update` as a `ScanIDDataclass`
        instance. `scandc_type` is not needed/used in this case. There
        is no data validation, since `ScanIDDataclass` does its own on
        initialization.
        """
        LOGGER.debug(f"replacing: ({coll=}) doc with {scan_id=} {dclass=}")

        async def find_one_and_update(update_dict: schema.StrDict) -> schema.StrDict:
            return await self._collections[coll].find_one_and_update(  # type: ignore[no-any-return]
                {"scan_id": scan_id},
                {"$set": update_dict},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

        # PARTIAL UPDATE
        if isinstance(update, dict):
            if not (dclass and dc.is_dataclass(dclass) and isinstance(dclass, type)):
                raise TypeError(
                    "for partial updates (where 'update' is a dict), 'dclass' must be a dataclass class/type"
                )
            fields = {x.name: x for x in dc.fields(dclass)}
            # enforce schema
            for key, value in update.items():
                try:
                    check_type(key, value, fields[key].type)  # TypeError, KeyError
                except (TypeError, KeyError) as e:
                    raise web.HTTPError(
                        500,
                        log_message=f"{e} [{coll=}, {scan_id=}]",
                    )
            # at this point we know all data is type checked, so transform & put in DB
            doc = await find_one_and_update(friendly_nested_asdict(update))
            out_type = dclass
        # WHOLE UPDATE
        else:
            doc = await find_one_and_update(dc.asdict(update))  # validate via dataclass
            out_type = type(update)

        # upsert
        if not doc:
            raise web.HTTPError(
                500,
                log_message=f"Failed to insert/update {coll} document ({scan_id})",
            )
        scandc = from_dict(out_type, doc)
        LOGGER.debug(f"replaced: ({coll=}) doc {scandc}")
        return scandc  # type: ignore[no-any-return]  # mypy internal bug


# -----------------------------------------------------------------------------


class ManifestClient(ScanIDCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def get(self, scan_id: str, incl_del: bool) -> schema.Manifest:
        """Get `schema.Manifest` using `scan_id`."""
        LOGGER.debug(f"getting manifest for {scan_id=}")
        manifest = await self._find_one(
            _MANIFEST_COLL_NAME, scan_id, schema.Manifest, incl_del
        )
        return manifest

    async def post(
        self,
        event_i3live_json_dict: schema.StrDict,
        scan_id: str,
        server_args: str,
        clientmanager_args: str,
        env_vars: dict[str, schema.StrDict],
    ) -> schema.Manifest:
        """Create `schema.Manifest` doc."""
        LOGGER.debug("creating new manifest")
        manifest = schema.Manifest(  # validates data
            scan_id=scan_id,
            is_deleted=False,
            event_i3live_json_dict=event_i3live_json_dict,
            server_args=server_args,
            clientmanager_args=clientmanager_args,
            env_vars=env_vars,
        )
        manifest = await self._upsert(_MANIFEST_COLL_NAME, manifest.scan_id, manifest)
        return manifest

    async def patch(
        self,
        scan_id: str,
        progress: schema.Progress | None,
        event_metadata: schema.EventMetadata | None,
        scan_metadata: schema.StrDict | None,
        condor_cluster: schema.CondorClutser | None,
    ) -> schema.Manifest:
        """Update `progress` at doc matching `scan_id`."""
        LOGGER.debug(f"patching manifest for {scan_id=}")

        if not (progress or event_metadata or scan_metadata or condor_cluster):
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return await self.get(scan_id, incl_del=True)

        upserting: schema.StrDict = {}

        # Store/validate: event_metadata & scan_metadata
        # NOTE: in theory there's a race condition (get+upsert), but it's set-once-only, so it's OK
        in_db = await self.get(scan_id, incl_del=True)
        # event_metadata
        if not event_metadata:
            pass  # don't put in DB
        elif not in_db.event_metadata:
            upserting["event_metadata"] = event_metadata
        elif in_db.event_metadata != event_metadata:
            msg = "Cannot change an existing event_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        # scan_metadata
        if not scan_metadata:
            pass  # don't put in DB
        elif not in_db.scan_metadata:
            upserting["scan_metadata"] = scan_metadata
        elif in_db.scan_metadata != scan_metadata:
            msg = "Cannot change an existing scan_metadata"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        # condor_cluster / condor_clusters
        if not condor_cluster:
            pass  # don't put in DB
        else:
            upserting["condor_clusters"] = in_db.condor_clusters + [condor_cluster]

        # progress
        if progress:
            upserting["progress"] = progress

        # put in DB
        if not upserting:  # did we actually update anything?
            LOGGER.debug(f"nothing to patch for manifest ({scan_id=})")
            return in_db
        LOGGER.debug(f"patching manifest for {scan_id=} with {upserting=}")
        manifest = await self._upsert(
            _MANIFEST_COLL_NAME,
            scan_id,
            upserting,
            schema.Manifest,
        )
        return manifest

    async def mark_as_deleted(self, scan_id: str) -> schema.Manifest:
        """Mark `schema.Manifest` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking manifest as deleted for {scan_id=}")

        manifest = await self._upsert(
            _MANIFEST_COLL_NAME, scan_id, {"is_deleted": True}, schema.Manifest
        )
        return manifest

    async def find_scan_ids(
        self,
        run_id: int,
        event_id: int,
        is_real_event: bool,
        incl_del: bool,
    ) -> AsyncIterator[str]:
        """Search over scans and find all matching runevent."""
        LOGGER.debug(
            f"finding: scan ids for {(run_id, event_id, is_real_event)=} ({incl_del=})"
        )

        # skip the dataclass-casting b/c we're just returning a str
        query = {
            "event_metadata.event_id": event_id,
            "event_metadata.run_id": run_id,
            "event_metadata.is_real_event": is_real_event,
            # NOTE: not searching for mjd
        }
        async for doc in self._collections[_MANIFEST_COLL_NAME].find(query):
            if not incl_del and doc["is_deleted"]:
                continue
            LOGGER.debug(
                f"found: {doc['scan_id']=} for {(run_id, event_id, is_real_event)=} ({incl_del=})"
            )
            yield doc["scan_id"]


# -----------------------------------------------------------------------------


class ResultClient(ScanIDCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def get(self, scan_id: str, incl_del: bool) -> schema.Result:
        """Get `schema.Result` using `scan_id`."""
        LOGGER.debug(f"getting result for {scan_id=}")
        result = await self._find_one(
            _RESULTS_COLL_NAME, scan_id, schema.Result, incl_del
        )
        return result

    async def put(
        self, scan_id: str, skyscan_result: schema.StrDict, is_final: bool
    ) -> schema.Result:
        """Override `schema.Result` at doc matching `scan_id`."""
        LOGGER.debug(f"overriding result for {scan_id=} {is_final=}")
        if not skyscan_result:
            msg = f"Attempted to add result with an empty object ({skyscan_result})"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        result = schema.Result(
            scan_id, False, skyscan_result, is_final
        )  # validates data
        result = await self._upsert(_RESULTS_COLL_NAME, result.scan_id, result)
        return result

    async def mark_as_deleted(self, scan_id: str) -> schema.Result:
        """Mark `schema.Result` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking result as deleted for {scan_id=}")

        result = await self._upsert(
            _RESULTS_COLL_NAME, scan_id, {"is_deleted": True}, schema.Result
        )
        return result
