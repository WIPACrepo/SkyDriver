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
    }

    DONT_CALL_IT_A_DB[workflow_id] = minimal_wf_doc

    return jsonify(
        {
            "workflow": minimal_wf_doc,
        }
    )


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(os.environ["EWMS_ADDRESS"].split(":")[-1]),
    )
