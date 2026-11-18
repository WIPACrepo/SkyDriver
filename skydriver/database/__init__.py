"""Init."""

from urllib.parse import quote_plus

from pymongo import AsyncMongoClient

from ..config import ENV
from . import interface, schema, utils
from .client import SkyDriverMongoValidatedDatabase

__all__ = [
    "SkyDriverMongoValidatedDatabase",
    "interface",
    "schema",
    "utils",
]


async def create_mongodb_client() -> AsyncMongoClient:  # type: ignore[valid-type]
    """Construct the MongoDB client."""
    auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)

    if auth_user and auth_pass:
        url = f"mongodb://{auth_user}:{auth_pass}@{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"
    else:
        url = f"mongodb://{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"

    mongo_client: AsyncMongoClient = AsyncMongoClient(url)
    return mongo_client
