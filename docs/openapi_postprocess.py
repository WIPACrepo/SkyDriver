"""Post-process openapi.json to convert OAS 3.1 multi-type arrays to anyOf.

sphinxcontrib-openapi 0.9.0 doesn't handle ``{"type": ["X", "Y", ...]}`` beyond
the simple nullable case ``["X", "null"]``. This script rewrites multi-type
arrays as ``anyOf: [{type: X}, ...]``, which is semantically equivalent and
valid in both OAS 3.0 and 3.1.
"""

import argparse
import json
import pathlib


def _convert(node: object) -> None:
    """Recursively rewrite multi-type arrays in-place as anyOf branches."""
    if isinstance(node, dict):
        t = node.get("type")
        # Only rewrite when `type` is a list with 2+ non-null entries.
        # Single-type-plus-null (nullable) stays as-is — sphinxcontrib-openapi handles that.
        if isinstance(t, list) and len([x for x in t if x != "null"]) >= 2:
            node["anyOf"] = [{"type": x} for x in t]
            del node["type"]
        for v in node.values():
            _convert(v)
    elif isinstance(node, list):
        for item in node:
            _convert(item)


def main() -> None:
    """Parse args, post-process the spec, and write it out."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=pathlib.Path, help="Path to openapi.json")
    parser.add_argument(
        "output",
        type=pathlib.Path,
        help="Path to output (can be the same as input for in-place rewrite)",
    )
    args = parser.parse_args()

    spec = json.loads(args.input.read_text())
    _convert(spec)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(spec, indent=2))


if __name__ == "__main__":
    main()
