"""Decorators for REST handlers"""

import functools
import logging
from collections.abc import Awaitable, Callable

import tornado.web
from rest_tools.server import token_attribute_role_mapping_auth
from wipac_dev_tools.mongo_jsonschema_tools import DocumentNotFoundException

from .config import INTERNAL_ACCT, USER_ACCT, is_testing

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# REST requestor auth


if is_testing():

    def service_account_auth(roles: list[str], **kwargs):  # type: ignore
        def make_wrapper(method):  # type: ignore[no-untyped-def]
            async def wrapper(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                LOGGER.warning("TESTING: auth disabled")
                self.auth_roles = [roles[0]]  # make as a list containing just 1st role
                return await method(self, *args, **kwargs)

            return wrapper

        return make_wrapper

else:
    service_account_auth = token_attribute_role_mapping_auth(  # type: ignore[no-untyped-call]
        role_attrs={
            USER_ACCT: [
                "resource_access.skydriver-external.roles=users",
            ],
            INTERNAL_ACCT: [
                "resource_access.skydriver-internal.roles=system",  # scanner & friends
            ],
        },
    )


# -----------------------------------------------------------------------------


def maybe_redirect_scan_id(roles: list[str]):
    """Decorator to redirect to the same handler with the replaced scan_id if scan replaced."""

    def decorator(method):
        @functools.wraps(method)
        async def wrapper(self, scan_id: str, *args, **kwargs):

            # only redirect for these roles
            if not any(r in self.auth_roles for r in roles):
                return await method(self, scan_id, *args, **kwargs)

            # skip if explicitly disabled
            if self.get_argument("no_redirect", default="false").lower() == "true":
                return await method(self, scan_id, *args, **kwargs)

            # look in DB
            replaced_by_scan_id = await self.db.manifests.find_one_field(
                {"scan_id": scan_id}, "replaced_by_scan_id"
            )
            if replaced_by_scan_id:
                # Reconstruct URL with replaced scan ID
                # Full URL: scheme://host/path?query
                new_url = self.request.full_url().replace(
                    scan_id, replaced_by_scan_id, 1
                )
                LOGGER.warning(f"replying with redirect for {scan_id} to {new_url}")
                return self.redirect(new_url)
            else:
                return await method(self, scan_id, *args, **kwargs)

        return wrapper

    return decorator


# -----------------------------------------------------------------------------


def http_404_on_document_not_found[**P, T](
    func: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]:
    """Decorator: catch `DocumentNotFoundException` from the wrapped async function
    and re-raise as `HTTPError(404)`.
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await func(*args, **kwargs)
        except DocumentNotFoundException as e:
            raise tornado.web.HTTPError(
                404,
                log_message=f"{e.__class__.__name__}: {e}",  # to stderr
                reason=f"Object not found in collection of {e.collection_name}.",  # to client
            ) from e

    return wrapper
