"""Tools for interacting with the mongo database."""

import copy
import logging

import jsonref
import tornado
from pymongo import AsyncMongoClient
from wipac_dev_tools.mongo_jsonschema_tools import (
    JSON,
    MongoJSONSchemaValidatedCollection,
)

from ..config import OPENAPI_DICT
from .utils import (
    _DB_NAME,
    _I3_EVENT_COLL_NAME,
    _MANIFEST_COLL_NAME,
    _RESULTS_COLL_NAME,
    _SCAN_BACKLOG_COLL_NAME,
    _SCAN_REQUEST_COLL_NAME,
    _SKYSCAN_K8S_JOB_COLL_NAME,
)


class OpenAPIToJSONSchema:
    """For plucking a JSONSchema subspec from an OpenAPI spec."""

    def __init__(self, openapi_dict: JSON):
        # resolve all $refs against the full doc once, so each plucked subspec is self-contained
        self.resolved_openapi_dict: JSON = jsonref.replace_refs(
            openapi_dict, proxies=False
        )

    def get_subspec(self, object_name: str) -> JSON:
        """Get a deep-copy of the JSONSchema spec for an 'component.schemas' object.

        Makes all root fields required.
        """
        try:
            subspec = copy.deepcopy(
                self.resolved_openapi_dict["components"]["schemas"][object_name]  # ty:ignore[not-subscriptable, invalid-argument-type]
            )
        except KeyError as e:
            raise ValueError(f"no JSONSchema spec found: {object_name}") from e

        subspec["required"] = list(subspec["properties"].keys())
        return subspec


class SkyDriverMongoValidatedDatabase:
    """Wraps a MongoDB client and collection clients with json schema validation."""

    def __init__(
        self,
        mongo_client: AsyncMongoClient,
        raise_500: bool,
        parent_logger: logging.Logger | None = None,
    ):
        self.mongo_client = mongo_client
        self.raise_500 = raise_500

        # don't persist this -- the subspecs are individually persisted
        openapi_to_jsonschema = OpenAPIToJSONSchema(OPENAPI_DICT)

        def _make(_col_name: str, _obj_name: str) -> MongoJSONSchemaValidatedCollection:
            return MongoJSONSchemaValidatedCollection(
                mongo_client[_DB_NAME][_col_name],
                openapi_to_jsonschema.get_subspec(_obj_name),
                parent_logger,
                lambda e: self._db_error_callback(e, _col_name),
            )

        # note: explicit typehints are only included so pycharm can pick these up
        self.manifests: MongoJSONSchemaValidatedCollection = _make(
            _MANIFEST_COLL_NAME, "Manifest"
        )
        self.results: MongoJSONSchemaValidatedCollection = _make(
            _RESULTS_COLL_NAME, "Result"
        )
        self.scan_backlog: MongoJSONSchemaValidatedCollection = _make(
            _SCAN_BACKLOG_COLL_NAME, "ScanBacklogEntry"
        )
        self.scan_requests: MongoJSONSchemaValidatedCollection = _make(
            _SCAN_REQUEST_COLL_NAME, "ScanRequestObj"
        )
        self.i3_events: MongoJSONSchemaValidatedCollection = _make(
            _I3_EVENT_COLL_NAME, "I3Event"
        )
        self.skyscan_k8s_jobs: MongoJSONSchemaValidatedCollection = _make(
            _SKYSCAN_K8S_JOB_COLL_NAME, "SkyscanK8sJob"
        )

    def _db_error_callback(self, exc: Exception, collection_name: str):
        """Handle a database error.

        If `self.raise_500=True`, raise a 500 error. Otherwise, return the exception.
           Technically, 500 errors are always raised when run in a tornado server, but
           `self.raise_500` provides custom messaging and earlier raising.
        """
        if self.raise_500:
            return tornado.web.HTTPError(
                status_code=500,
                log_message=f"{exc.__class__.__name__}: {exc}",  # to stderr
                reason=f"Internal database error for collection='{collection_name}'",  # to client
            )
        else:
            return exc
