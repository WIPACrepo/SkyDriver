"""A dummy EWMS server for testing."""

import os
import uuid
from typing import Any

from flask import Flask, jsonify

app = Flask(__name__)

DONT_CALL_IT_A_DB: dict[str, Any] = {}


@app.route("/v0/workflows", methods=["POST"])
def dummy_workflows_post():
    # in the real ewms, there's a bunch of db logic, etc.

    workflow_id = uuid.uuid4().hex
    minimal_wf_doc = {
        "workflow_id": workflow_id,
        "deactivated": None,
        # add more fields only if needed in tests--keep things simple
    }

    DONT_CALL_IT_A_DB[workflow_id] = minimal_wf_doc

    return jsonify(
        {
            "workflow": minimal_wf_doc,
        }
    )


@app.route("/v0/workflows/<workflow_id>", methods=["GET"])
def dummy_workflows_get(workflow_id: str):
    return jsonify(DONT_CALL_IT_A_DB[workflow_id])


@app.route("/v0/workflows/<workflow_id>/actions/abort", methods=["POST"])
def dummy_workflows_abort(workflow_id: str):
    DONT_CALL_IT_A_DB[workflow_id].update({"deactivated": "abort"})
    return jsonify({})


@app.route("/v0/workflows/<workflow_id>/actions/finished", methods=["POST"])
def dummy_workflows_finished(workflow_id: str):
    DONT_CALL_IT_A_DB[workflow_id].update({"deactivated": "finished"})
    return jsonify({})


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(os.environ["EWMS_ADDRESS"].split(":")[-1]),
    )
