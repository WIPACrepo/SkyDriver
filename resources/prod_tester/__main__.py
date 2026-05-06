"""Main entrypoint."""

import asyncio

from . import test_suit_prod

if __name__ == "__main__":
    asyncio.run(test_suit_prod.main())
