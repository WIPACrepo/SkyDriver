"""A dummy EWMS server for testing."""

import os
from typing import Any

from flask import Flask, jsonify

app = Flask(__name__)

DONT_CALL_IT_A_DB: dict[str, Any] = {}


@app.route("/v0/mqs/workflows/<workflow_id>/mq-group/activation", methods=["POST"])
def dummy_mq_group_activation_post(workflow_id: str):
    # in the real mqs, there's a bunch of db logic, etc.

    stored = DONT_CALL_IT_A_DB[workflow_id]
    for mqprofile in stored["mqprofiles"]:
        mqprofile["is_activated"] = True
        mqprofile["auth_token"] = "DUMMY_TOKEN"
        mqprofile["broker_type"] = "DUMMY_BROKER_TYPE"
        mqprofile["broker_address"] = "DUMMY_BROKER_ADDRESS"

    return jsonify(stored)


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(os.environ["EWMS_ADDRESS"].split(":")[-1]),
    )
