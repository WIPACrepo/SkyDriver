"""Fixtures."""


import socket
from typing import Any

import pytest
import pytest_asyncio
from skydriver.database import create_mongodb_client
from skydriver.database.interface import drop_collections


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port


@pytest_asyncio.fixture
async def mongo_clear() -> Any:
    """Clear the MongoDB after test completes."""
    motor_client = await create_mongodb_client()
    try:
        await drop_collections(motor_client)
        yield
    finally:
        await drop_collections(motor_client)
