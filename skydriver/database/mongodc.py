"""Wrappers around pymongo / motor with dataclass wrappers."""

import dataclasses as dc
import logging
from typing import TYPE_CHECKING, Any, AsyncIterator, Type, TypeVar

import typeguard
from dacite import from_dict
from motor.motor_asyncio import AsyncIOMotorCollection

if TYPE_CHECKING:
    from _typeshed import DataclassInstance  # type: ignore[attr-defined]
else:
    DataclassInstance = Any

DataclassT = TypeVar("DataclassT", bound=DataclassInstance)

LOGGER = logging.getLogger(__name__)


class DocumentNotFoundException(Exception):
    """Raised when document is not found for a particular query."""


class MotorDataclassCollection(AsyncIOMotorCollection):  # type: ignore[misc, valid-type]
    """A wrapper around motor's collection with dataclass typecasting."""

    async def find(
        self,
        *args: Any,
        return_dclass: Type[DataclassT],
        **kwargs: Any,
    ) -> AsyncIterator[DataclassT]:
        async for doc in super().find(*args, **kwargs):
            if return_dclass == dict:
                yield doc
            else:
                yield from_dict(return_dclass, doc)

    async def find_one(
        self,
        *args: Any,
        return_dclass: Type[DataclassT],
        **kwargs: Any,
    ) -> DataclassT:
        res = await super().find_one(*args, **kwargs)
        if not res:
            raise DocumentNotFoundException()
        return from_dict(return_dclass, res)

    async def find_one_and_update(
        self,
        *args: Any,
        return_dclass: Type[DataclassT],
        **kwargs: Any,
    ) -> DataclassT:
        res = await super().find_one_and_update(*args, **kwargs)
        if not res:
            raise DocumentNotFoundException()
        return from_dict(return_dclass, res)


def friendly_nested_asdict(value: Any) -> Any:
    """Convert any founded nested dataclass to dict if applicable.

    Like `dc.asdict()` but safe for any type and list- and dict-
    friendly.
    """
    if isinstance(value, dict):
        return {k: friendly_nested_asdict(v) for k, v in value.items()}

    if isinstance(value, list):
        return [friendly_nested_asdict(v) for v in value]

    if dc.is_dataclass(value):
        return dc.asdict(value)

    return value


def typecheck_as_dc_fields(dicto: dict, dclass: Type[DataclassT]) -> dict:
    """Type-check dict fields as if they were dataclass fields."""
    if not (dclass and dc.is_dataclass(dclass) and isinstance(dclass, type)):
        raise TypeError("'dclass' must be a dataclass class/type")
    fields = {x.name: x for x in dc.fields(dclass)}
    # enforce schema
    for key, value in dicto.items():
        try:
            typeguard.check_type(value, fields[key].type)  # TypeError, KeyError
        except (typeguard.TypeCheckError, KeyError) as e:
            LOGGER.exception(e)
            raise TypeError(f"Invalid type (field '{key}')") from e

    # at this point we know all data is type checked, so transform
    return friendly_nested_asdict(dicto)  # type: ignore[no-any-return]
