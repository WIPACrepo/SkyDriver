"""Integration tests for the openapi endpoint and spec."""

import json
from typing import Any, Callable

import pytest
from rest_tools.client import RestClient
from rest_tools.openapi_tools import request_and_validate
from skydriver.config import OPENAPI_DICT, OPENAPI_PATH, OPENAPI_SPEC


def print_it(obj: Any) -> None:
    print(json.dumps(obj, indent=4))


@pytest.mark.order("first")  # check any issues with the spec before other route tests
async def test_00(server: Callable[[], RestClient]) -> None:
    """Test backlog job starting."""
    rc = server()

    # get spec from server endpoint
    spec_from_resp = await rc.request("GET", "/openapi.json")
    for k in list(spec_from_resp["info"].keys()):  # so to change size during iteration
        # check that the schema was populated correctly
        assert spec_from_resp["info"][k], (
            f"full info fields: {spec_from_resp['info']!r}"
        )
        # don't include extra 'info' fields populated @ runtime
        if k not in ("title", "version"):
            spec_from_resp["info"].pop(k)

    # get spec from disk
    with open(OPENAPI_PATH, "rb") as f:
        spec_on_disk = json.load(f)

    # assert all the specs are the same
    assert spec_on_disk == spec_from_resp  # disk vs response
    assert spec_on_disk == OPENAPI_DICT  # disk vs (server) in-memory

    # now, for fun, let's validate the getter's response
    await request_and_validate(rc, OPENAPI_SPEC, "GET", "/openapi.json")
