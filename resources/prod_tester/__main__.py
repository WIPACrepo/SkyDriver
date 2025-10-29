"""Main entrypoint."""

import asyncio
import subprocess
import sys
from pathlib import Path

from . import test_suit_prod

if __name__ == "__main__":
    subprocess.check_call("python -m pip install --upgrade pip".split())
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "-r",
            str(Path(__file__).parent / "requirements.txt"),
        ]
    )
    asyncio.run(test_suit_prod.main())
