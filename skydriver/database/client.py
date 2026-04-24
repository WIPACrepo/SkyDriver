"""Tools for interacting with the mongo database."""

import copy
import logging

import tornado
from pymongo import AsyncMongoClient
from wipac_dev_tools.mongo_jsonschema_tools import MongoJSONSchemaValidatedCollection

from .utils import (
    _DB_NAME,
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


class MQSMongoValidatedDatabase:
    """Wraps a MongoDB client and collection clients with json schema validation."""

    def __init__(
        self,
        mongo_client: AsyncMongoClient,
        do_send_500s_to_client: bool,
        parent_logger: logging.Logger | None = None,
    ):
        self.mongo_client = mongo_client
        self.do_send_500s_to_client = do_send_500s_to_client

        self.manifests = MongoJSONSchemaValidatedCollection(
            mongo_client[_DB_NAME][_MANIFEST_COLL_NAME],
            get_jsonschema_subspec_from_openapi("Manifest"),
            parent_logger,
            lambda e: self._db_error_callback(e, _MANIFEST_COLL_NAME),
        )

        self.results = MongoJSONSchemaValidatedCollection(
            mongo_client[_DB_NAME][_RESULTS_COLL_NAME],
            get_jsonschema_subspec_from_openapi("Result"),
            parent_logger,
            lambda e: self._db_error_callback(e, _MANIFEST_COLL_NAME),
        )

        self.scan_backlog = MongoJSONSchemaValidatedCollection(
            mongo_client[_DB_NAME][_SCAN_BACKLOG_COLL_NAME],
            get_jsonschema_subspec_from_openapi("ScanBacklogEntry"),
            parent_logger,
            lambda e: self._db_error_callback(e, _MANIFEST_COLL_NAME),
        )

        self.scan_requests = MongoJSONSchemaValidatedCollection(
            mongo_client[_DB_NAME][_SCAN_REQUEST_COLL_NAME],
            get_jsonschema_subspec_from_openapi("ScanRequestObj"),
            parent_logger,
            lambda e: self._db_error_callback(e, _MANIFEST_COLL_NAME),
        )

        _I3_EVENT_COLL_NAME = "I3Events"

        self.skyscan_k8s_jobs = MongoJSONSchemaValidatedCollection(
            mongo_client[_DB_NAME][_SKYSCAN_K8S_JOB_COLL_NAME],
            get_jsonschema_subspec_from_openapi("SkyscanK8sJob"),
            parent_logger,
            lambda e: self._db_error_callback(e, _MANIFEST_COLL_NAME),
        )

    def _db_error_callback(self, exc: Exception, collection_name: str):
        if self.do_send_500s_to_client:
            return tornado.web.HTTPError(
                status_code=500,
                log_message=f"{exc.__class__.__name__}: {exc}",  # to stderr
                reason=f"Internal database error for collection='{collection_name}'",  # to client
            )
        else:
            return exc
