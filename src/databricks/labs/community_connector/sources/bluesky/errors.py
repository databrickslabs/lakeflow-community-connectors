"""Sanitized, actionable failures: never include response bodies or credentials."""


class JetstreamError(RuntimeError):
    """Base exception for failed Jetstream refreshes.

    Notes
    -----
    A failed refresh must not commit its candidate offset.
    """


class ProtocolError(JetstreamError):
    """An unsupported or corrupt server response was encountered.

    Notes
    -----
    Raised when a response violates the supported Jetstream archive protocol.
    """


class CursorTooOld(JetstreamError):
    """The requested archive range is no longer available.

    Notes
    -----
    The connector does not skip data or silently reset an expired cursor.
    """


class ArchiveChanged(JetstreamError):
    """The planned archive generation is unavailable for replay.

    Notes
    -----
    Retrying cannot safely replace the recorded plan with a new archive generation.
    """


class RefreshTimeout(JetstreamError):
    """The bounded request or refresh time budget was exhausted.

    Notes
    -----
    Retain the previous checkpoint and retry in a later refresh.
    """
