"""Database interface for persisted scan data."""

import dataclasses as dc
from typing import TYPE_CHECKING, Any, AsyncIterator, Type, TypeVar, cast

import typeguard
from dacite import from_dict  # type: ignore[attr-defined]
from motor.motor_asyncio import (  # type: ignore
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
)
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from tornado import web

from ..config import ENV, LOGGER
from . import schema

if TYPE_CHECKING:
    from _typeshed import DataclassInstance  # type: ignore[attr-defined]
else:
    DataclassInstance = Any


DataclassT = TypeVar("DataclassT", bound=DataclassInstance)


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
_SCAN_BACKLOG_COLL_NAME = "ScanBacklog"


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

    # SCAN BACKLOG COLL
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].create_index(
        [("timestamp", ASCENDING)],
        name="timestamp_index",
        unique=False,
    )
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].create_index(
        "scan_id",
        name="scan_id_index",
        unique=True,
    )


async def drop_collections(motor_client: AsyncIOMotorClient) -> None:
    """Drop the "regular" collections -- most useful for testing."""
    if not ENV.CI_TEST:
        raise RuntimeError("Cannot drop collections if not in testing mode")
    await motor_client[_DB_NAME][_MANIFEST_COLL_NAME].drop()
    await motor_client[_DB_NAME][_RESULTS_COLL_NAME].drop()
    await motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME].drop()


class DataclassCollectionFacade:
    """Motor Client wrapper w/ guardrails & `dataclasses.dataclass` casting."""

    def __init__(self, motor_client: AsyncIOMotorClient) -> None:
        # place in a dictionary so there's some safeguarding against bogus collections
        self._collections: dict[str, AsyncIOMotorCollection] = {
            _MANIFEST_COLL_NAME: motor_client[_DB_NAME][_MANIFEST_COLL_NAME],
            _RESULTS_COLL_NAME: motor_client[_DB_NAME][_RESULTS_COLL_NAME],
            _SCAN_BACKLOG_COLL_NAME: motor_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME],
        }

    async def _find_one(
        self,
        coll: str,
        query: dict[str, Any],
        dclass: Type[DataclassT],
        incl_del: bool,
    ) -> DataclassT:
        """Get document by 'query'."""
        LOGGER.debug(f"finding: ({coll=}) doc with {query=} for {dclass=}")
        doc = await self._collections[coll].find_one(query)
        if (not doc) or (doc["is_deleted"] and not incl_del):
            raise web.HTTPError(
                404,
                log_message=f"Document Not Found: {coll} document ({query})",
            )
        dc_doc = from_dict(dclass, doc)
        LOGGER.debug(f"found: ({coll=}) doc {dc_doc}")
        return dc_doc  # type: ignore[no-any-return]  # mypy internal bug

    async def _upsert(
        self,
        coll: str,
        query: dict[str, Any],
        update: schema.StrDict | DataclassT,
        dclass: Type[DataclassT] | None = None,
    ) -> DataclassT:
        """Insert/update the doc.

        *For partial updates:* pass `update` as a `dict` along with a
        `dclass` (`dataclasses.dataclass` class/type). `dclass` is
        used to validate updates against the document's schema and casts
        the returned document.

        *For whole inserts:* pass `update` as a `dataclass`
        instance. `dclass` is not needed/used in this case. There
        is no data validation--it's assumed this is pre-validated.
        """
        LOGGER.debug(f"replacing: ({coll=}) doc with {query=} {dclass=}")

        async def find_one_and_update(update_dict: schema.StrDict) -> schema.StrDict:
            return await self._collections[coll].find_one_and_update(  # type: ignore[no-any-return]
                query,
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
                    typeguard.check_type(value, fields[key].type)  # TypeError, KeyError
                except (typeguard.TypeCheckError, KeyError) as e:
                    LOGGER.exception(e)
                    msg = f"Invalid type (field '{key}')"
                    raise web.HTTPError(
                        422,
                        log_message=msg + f" for {query=}",
                        reason=msg,
                    )
            # at this point we know all data is type checked, so transform & put in DB
            doc = await find_one_and_update(friendly_nested_asdict(update))
            out_type = dclass
        # WHOLE UPDATE
        else:
            try:  # validate via dataclass's `@typechecked` wrapper
                doc = await find_one_and_update(dc.asdict(update))
            except typeguard.TypeCheckError as e:
                LOGGER.exception(e)
                raise web.HTTPError(
                    422,
                    log_message=f"Invalid type for {query=}",
                    reason="Invalid type",
                )
            out_type = type(update)

        # upsert
        if not doc:
            raise web.HTTPError(
                500,
                log_message=f"Failed to insert/update {coll} document ({query})",
            )
        dc_doc = from_dict(out_type, doc)
        LOGGER.debug(f"replaced: ({coll=}) doc {dc_doc}")
        return cast(DataclassT, dc_doc)  # mypy internal bug


# -----------------------------------------------------------------------------


class ManifestClient(DataclassCollectionFacade):
    """Wraps the attribute for the metadata of a scan."""

    async def get(self, scan_id: str, incl_del: bool) -> schema.Manifest:
        """Get `schema.Manifest` using `scan_id`."""
        LOGGER.debug(f"getting manifest for {scan_id=}")
        manifest = await self._find_one(
            _MANIFEST_COLL_NAME,
            {"scan_id": scan_id},
            schema.Manifest,
            incl_del,
        )
        return manifest

    async def post(
        self,
        event_i3live_json_dict: schema.StrDict,
        scan_id: str,
        scanner_server_args: str,
        tms_args_list: list[str],
        env_vars: dict[str, schema.StrDict],
    ) -> schema.Manifest:
        """Create `schema.Manifest` doc."""
        LOGGER.debug("creating new manifest")
        manifest = schema.Manifest(  # validates data
            scan_id=scan_id,
            is_deleted=False,
            event_i3live_json_dict=event_i3live_json_dict,
            scanner_server_args=scanner_server_args,
            tms_args=tms_args_list,
            env_vars=env_vars,
        )
        manifest = await self._upsert(
            _MANIFEST_COLL_NAME,
            {"scan_id": manifest.scan_id},
            manifest,
        )
        return manifest

    async def patch(
        self,
        scan_id: str,
        progress: schema.Progress | None = None,
        event_metadata: schema.EventMetadata | None = None,
        scan_metadata: schema.StrDict | None = None,
        condor_cluster: schema.CondorClutser | None = None,
        complete: bool | None = None,
    ) -> schema.Manifest:
        """Update `progress` at doc matching `scan_id`."""
        LOGGER.debug(f"patching manifest for {scan_id=}")

        if not (
            progress
            or event_metadata
            or scan_metadata
            or condor_cluster
            or complete is not None  # True/False is ok
        ):
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

        # complete
        if complete is not None:
            upserting["complete"] = complete

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
            {"scan_id": scan_id},
            upserting,
            schema.Manifest,
        )
        return manifest

    async def mark_as_deleted(self, scan_id: str) -> schema.Manifest:
        """Mark `schema.Manifest` at doc matching `scan_id` as deleted."""
        LOGGER.debug(f"marking manifest as deleted for {scan_id=}")

        manifest = await self._upsert(
            _MANIFEST_COLL_NAME,
            {"scan_id": scan_id},
            {"is_deleted": True},
            schema.Manifest,
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


class ResultClient(DataclassCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def get(self, scan_id: str, incl_del: bool) -> schema.Result:
        """Get `schema.Result` using `scan_id`."""
        LOGGER.debug(f"getting result for {scan_id=}")
        result = await self._find_one(
            _RESULTS_COLL_NAME,
            {"scan_id": scan_id},
            schema.Result,
            incl_del,
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
        result = schema.Result(scan_id, skyscan_result, is_final)  # validates data
        result = await self._upsert(
            _RESULTS_COLL_NAME,
            {"scan_id": result.scan_id},
            result,
        )
        return result


# -----------------------------------------------------------------------------


class DocumentNotFoundException(Exception):
    """Raised when document is not found for a particular query."""


class ScanBacklogClient(DataclassCollectionFacade):
    """Wraps the attribute for the result of a scan."""

    async def peek(self) -> schema.ScanBacklogEntry:
        """Get oldest `schema.ScanBacklogEntry`."""
        LOGGER.debug("peeking ScanBacklogEntry")

        doc = (
            await self._collections[_SCAN_BACKLOG_COLL_NAME]
            .find({})
            .sort([("timestamp", ASCENDING)])
            .limit(1)
        )
        if not doc:
            raise DocumentNotFoundException()

        entry = from_dict(schema.ScanBacklogEntry, doc)
        LOGGER.debug(f"found: ({_SCAN_BACKLOG_COLL_NAME=}) doc {entry}")
        return entry  # type: ignore[no-any-return]  # mypy internal bug

    async def remove(self, entry: schema.ScanBacklogEntry) -> schema.ScanBacklogEntry:
        """Remove entry, `schema.ScanBacklogEntry`."""
        LOGGER.debug("removing ScanBacklogEntry")
        res = await self._collections[_SCAN_BACKLOG_COLL_NAME].delete_one(
            {"scan_id": entry.scan_id}
        )
        LOGGER.debug(f"delete_one result: {res}")
        return entry

    async def insert(self, entry: schema.ScanBacklogEntry) -> None:
        """Insert entry, `schema.ScanBacklogEntry`."""
        LOGGER.debug(f"inserting {entry=}")
        doc = dc.asdict(entry)
        res = await self._collections[_SCAN_BACKLOG_COLL_NAME].insert_one(doc)
        LOGGER.debug(f"insert result: {res}")
        LOGGER.debug(f"Inserted backlog entry for {entry.scan_id=}")
