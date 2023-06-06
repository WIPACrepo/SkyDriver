"""Init."""


from urllib.parse import quote_plus

from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore

from ..config import ENV


async def create_mongodb_client() -> AsyncIOMotorClient:
    """Construct the MongoDB client."""
    auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)

    if auth_user and auth_pass:
        url = f"mongodb://{auth_user}:{auth_pass}@{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"
    else:
        url = f"mongodb://{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"

    mongo_client = AsyncIOMotorClient(url)
    return mongo_client
