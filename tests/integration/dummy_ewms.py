"""A dummy EWMS server for testing."""

import os
import pprint
import uuid
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

_URL_V_PREFIX = "v1"

DONT_CALL_IT_A_DB__WORKFLOWS: dict[str, Any] = {}


@app.route(f"/{_URL_V_PREFIX}/workflows", methods=["POST"])
def dummy_workflows_post():
    # in the real ewms, there's a bunch of db logic, etc.

    # IRL, we'd do something with this, but this isn't real life
    req_json = request.get_json()
    pprint.pprint(req_json)

    # "make" a workflow
    workflow_id = uuid.uuid4().hex
    minimal_wf_doc = {
        "workflow_id": workflow_id,
        "deactivated": None,
        # add more fields only if needed in tests--keep things simple
    }

    DONT_CALL_IT_A_DB__WORKFLOWS[workflow_id] = minimal_wf_doc

    return jsonify(
        {
            "workflow": minimal_wf_doc,
        }
    )


@app.route(f"/{_URL_V_PREFIX}/workflows/<workflow_id>", methods=["GET"])
def dummy_workflows_get(workflow_id: str):
    return jsonify(DONT_CALL_IT_A_DB__WORKFLOWS[workflow_id])


@app.route(f"/{_URL_V_PREFIX}/workflows/<workflow_id>/actions/abort", methods=["POST"])
def dummy_workflows_abort(workflow_id: str):
    DONT_CALL_IT_A_DB__WORKFLOWS[workflow_id].update({"deactivated": "abort"})
    return jsonify({})


@app.route(
    f"/{_URL_V_PREFIX}/workflows/<workflow_id>/actions/finished", methods=["POST"]
)
def dummy_workflows_finished(workflow_id: str):
    DONT_CALL_IT_A_DB__WORKFLOWS[workflow_id].update({"deactivated": "finished"})
    return jsonify({})


@app.route(f"/{_URL_V_PREFIX}/query/taskforces", methods=["POST"])
def dummy_query_taskforces():
    req_json = request.get_json()
    pprint.pprint(req_json)

    # respond with correctly-syntaxed gibberish
    resp = {
        "taskforces": [
            {
                "taskforce_uuid": f"TF-{req_json['query']['workflow_id']}",
                "phase": "the-best-phase-ever",
                # IRL, there are other attrs here
            }
        ]
    }
    return jsonify(resp)


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(os.environ["EWMS_ADDRESS"].split(":")[-1]),
    )
