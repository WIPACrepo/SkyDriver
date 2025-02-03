"""Wrappers around pymongo / motor with dataclass wrappers."""

import dataclasses as dc
import logging
import time
from typing import Any, AsyncIterator, Type, TypeVar

import dacite
import typeguard
from dacite import from_dict
from motor.motor_asyncio import AsyncIOMotorCollection

T = TypeVar("T")

LOGGER = logging.getLogger(__name__)


class DocumentNotFoundException(Exception):
    """Raised when document is not found for a particular query."""


class MotorDataclassCollection(AsyncIOMotorCollection):  # type: ignore[misc, valid-type]
    """A wrapper around motor's collection with dataclass typecasting."""

    async def find(
        self,
        *args: Any,
        return_dclass: Type[T],
        **kwargs: Any,
    ) -> AsyncIterator[T]:
        """Wraps `AsyncIOMotorCollection.find()` and typecasts the result as a
        dataclass.

        Additional Keyword Arguments:
            `return_dclass` -- the dataclass to cast the return type;
                alternatively, this can be any type as long as the mongo
                function returns that type directly (ex: `dict`)
        """
        async for doc in super().find(*args, **kwargs):
            if isinstance(doc, return_dclass):
                yield doc
            else:
                yield from_dict(return_dclass, doc)

    async def find_one(
        self,
        *args: Any,
        return_dclass: Type[T],
        **kwargs: Any,
    ) -> T:
        """Wraps `AsyncIOMotorCollection.find_one()` and typecasts the result
        as a dataclass.

        Additional Keyword Arguments:
            `return_dclass` -- the dataclass to cast the return type;
                alternatively, this can be any type as long as the mongo
                function returns that type directly (ex: `dict`)
        """
        doc = await super().find_one(*args, **kwargs)
        if not doc:
            raise DocumentNotFoundException()
        if isinstance(doc, return_dclass):
            return doc
        else:
            try:
                return from_dict(return_dclass, doc)
            except dacite.exceptions.DaciteError as e:
                logging.error(repr(e))
                logging.error("dacite validation failed on the following object:")
                logging.error(doc)
                raise e

    async def find_one_and_update(
        self,
        filter: dict[str, Any],
        update: dict[str, Any],
        *args: Any,
        return_dclass: Type[T],
        **kwargs: Any,
    ) -> T:
        """Wraps `AsyncIOMotorCollection.find_one_and_update()` and typecasts
        the result as a dataclass.

        Additional Keyword Arguments:
            `return_dclass` -- the dataclass to cast the return type;
                alternatively, this can be any type as long as the mongo
                function returns that type directly (ex: `dict`)
        """
        if (
            "$set" in update
            and dc.is_dataclass(return_dclass)
            and "last_updated" in [f.name for f in dc.fields(return_dclass)]
        ):
            update["$set"].update({"last_updated": time.time()})

        doc = await super().find_one_and_update(filter, update, *args, **kwargs)
        if not doc:
            raise DocumentNotFoundException()
        if isinstance(doc, return_dclass):
            return doc
        else:
            return from_dict(return_dclass, doc)


def friendly_nested_asdict(value: Any) -> Any:
    """Convert any founded nested dataclass to dict if applicable.

    Like `dc.asdict()` but safe for any type and list- and dict-
    friendly.
    """
    if isinstance(value, dict):
        return {k: friendly_nested_asdict(v) for k, v in value.items()}

    if isinstance(value, list):
        return [friendly_nested_asdict(v) for v in value]

    try:
        return dc.asdict(value)
    except TypeError:  # happens when the value is not a dataclass instance
        pass

    return value


def typecheck_as_dc_fields(dicto: dict, dclass: Type[T]) -> dict:
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
