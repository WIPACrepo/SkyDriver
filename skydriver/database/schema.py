"""Collection of dataclass-based schema for the database."""

import wipac_dev_tools as wdt


class ReadOnlyDotDict(dict):
    """A dict subclass that allows recursive attribute-style read access."""

    def __getattr__(self, name: str) -> object:
        try:
            value = self[name]
        except KeyError:
            raise AttributeError(name)
        # Wrap nested dicts lazily on access
        if isinstance(value, dict) and not isinstance(value, ReadOnlyDotDict):
            return ReadOnlyDotDict(value)
        return value


def obfuscate_cl_args(args: str) -> str:
    """Obfuscate any sensitive strings in the command line arguments."""
    # first, check if any sensitive strings (searches using substrings)
    if not wdt.data_safety_tools.is_name_sensitive(args):
        return args
    # now, go one-by-one
    out_args: list[str] = []
    current_option = ""
    for string in args.split():
        if string.startswith("--"):  # ex: --foo -> "... --foo bar baz ..."
            current_option = string
            out_args += string
        elif current_option:  # ex: baz -> "... --foo bar baz ..."
            out_args += wdt.data_safety_tools.obfuscate_value_if_sensitive(
                current_option, string
            )
        else:  # ex: my_module -> "python -m my_module ... --foo bar baz ..."
            out_args += string
    return " ".join(out_args)


_NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS = "not-yet-requested"
# ^^^ don't use in boolean logic directly -- use 'has_skydriver_requested_ewms_workflow()'

DEPRECATED_EVENT_I3LIVE_JSON_DICT = "use 'i3_event_id'"
DEPRECATED_EWMS_TASK = "use 'ewms_workflow_id'"


def has_skydriver_requested_ewms_workflow(ewms_workflow_id: str | None) -> bool:
    """Figure out if 'ewms_workflow_id' indicates is an actual EWMS workflow id."""
    return bool(
        ewms_workflow_id
        not in [
            None,  # old scans (v1)
            _NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,  # pending scans
        ]
    )
