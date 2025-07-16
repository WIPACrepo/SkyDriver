"""Decorators for REST handlers"""

import functools
import logging

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
            manifest = await self.manifests.get(scan_id, incl_del=True)
            if manifest.replaced_by_scan_id:
                # Reconstruct URL with replaced scan ID
                # Full URL: scheme://host/path?query
                new_url = self.request.full_url().replace(
                    scan_id, manifest.replaced_by_scan_id, 1
                )
                LOGGER.warning(f"replying with redirect for {scan_id} to {new_url}")
                return self.redirect(new_url)
            else:
                return await method(self, scan_id, *args, **kwargs)

        return wrapper

    return decorator
