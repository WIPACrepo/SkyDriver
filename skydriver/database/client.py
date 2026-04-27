"""Tools for interacting with the mongo database."""

import copy
import logging
from typing import Any

import tornado
from pymongo import AsyncMongoClient
from wipac_dev_tools.mongo_jsonschema_tools import (
    DocumentNotFoundException,
    MongoJSONSchemaValidatedCollection,
)

from .utils import (
    _DB_NAME,
    _I3_EVENT_COLL_NAME,
    _MANIFEST_COLL_NAME,
    _RESULTS_COLL_NAME,
    _SCAN_BACKLOG_COLL_NAME,
    _SCAN_REQUEST_COLL_NAME,
    _SKYSCAN_K8S_JOB_COLL_NAME,
)
from ..config import OPENAPI_DICT


def get_jsonschema_subspec_from_openapi(object_name: str) -> dict[str, Any]:
    """Get a deep-copy of the JSONSchema spec for an 'component.schemas' object.

    Makes all root fields required.
    """
    try:
        subspec = copy.deepcopy(OPENAPI_DICT["components"]["schemas"][object_name])
    except KeyError as e:
        raise ValueError(f"no JSONSchema spec found: {object_name}") from e

    subspec["required"] = list(subspec["properties"].keys())
    return subspec


class SkyDriverMongoValidatedDatabase:
    """Wraps a MongoDB client and collection clients with json schema validation."""

    def __init__(
        self,
        mongo_client: AsyncMongoClient,
        send_web_errors: bool,
        parent_logger: logging.Logger | None = None,
    ):
        self.mongo_client = mongo_client
        self.send_web_errors = send_web_errors

        def _make(_col_name: str, _obj_name: str) -> MongoJSONSchemaValidatedCollection:
            return MongoJSONSchemaValidatedCollection(
                mongo_client[_DB_NAME][_col_name],
                get_jsonschema_subspec_from_openapi(_obj_name),
                parent_logger,
                lambda e: self._db_error_callback(e, _col_name),
            )

        self.manifests = _make(_MANIFEST_COLL_NAME, "Manifest")
        self.results = _make(_RESULTS_COLL_NAME, "Result")
        self.scan_backlog = _make(_SCAN_BACKLOG_COLL_NAME, "ScanBacklogEntry")
        self.scan_requests = _make(_SCAN_REQUEST_COLL_NAME, "ScanRequestObj")
        self.i3_events = _make(_I3_EVENT_COLL_NAME, "I3Event")
        self.skyscan_k8s_jobs = _make(_SKYSCAN_K8S_JOB_COLL_NAME, "SkyscanK8sJob")

    def _db_error_callback(self, exc: Exception, collection_name: str):
        """Handle a database error.

        If `self.send_web_errors=True`, raise a 400/500 errors. Otherwise, return the exception.
           Technically, 500 errors are always raised when run in a tornado server, but
           this provides custom messaging and earlier raising.
        """
        if self.send_web_errors:
            if isinstance(exc, DocumentNotFoundException):
                return tornado.web.HTTPError(
                    404,
                    log_message=f"{exc.__class__.__name__}: {exc}",  # to stderr
                    reason=f"Document Not Found: {collection_name} document",  # to client
                )
            else:
                return tornado.web.HTTPError(
                    status_code=500,
                    log_message=f"{exc.__class__.__name__}: {exc}",  # to stderr
                    reason=f"Internal database error for collection='{collection_name}'",  # to client
                )
        else:
            return exc
