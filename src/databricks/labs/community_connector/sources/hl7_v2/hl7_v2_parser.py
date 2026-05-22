"""Lightweight zero-dependency HL7 v2 message parser.

Handles HL7 v2.1–v2.9 pipe-delimited messages. No external libraries required.

Usage:
    from hl7_parser import parse_message

    msg = parse_message(raw_text)
    if msg:
        msh = msg.get_segment("MSH")
        msg_id = msh.get_field(10)         # MSH-10: Message Control ID
        msg_type = msh.get_component(9, 1)  # MSH-9.1: Message Code (e.g. "ADT")
"""

from __future__ import annotations

_DEFAULT_FIELD_SEP = "|"
_DEFAULT_COMP_SEP = "^"
_DEFAULT_REP_SEP = "~"
_DEFAULT_ESC_CHAR = "\\"
_DEFAULT_SUB_COMP_SEP = "&"


class HL7EncodingChars:
    """Encoding characters read from the MSH segment."""

    __slots__ = ("field_sep", "comp_sep", "rep_sep", "esc_char", "sub_comp_sep")

    def __init__(
        self,
        field_sep: str = _DEFAULT_FIELD_SEP,
        comp_sep: str = _DEFAULT_COMP_SEP,
        rep_sep: str = _DEFAULT_REP_SEP,
        esc_char: str = _DEFAULT_ESC_CHAR,
        sub_comp_sep: str = _DEFAULT_SUB_COMP_SEP,
    ) -> None:
        self.field_sep = field_sep
        self.comp_sep = comp_sep
        self.rep_sep = rep_sep
        self.esc_char = esc_char
        self.sub_comp_sep = sub_comp_sep


def _decode_escape(value: str, enc: HL7EncodingChars) -> str:
    """Decode HL7 escape sequences.

    Standard sequences:
      \\F\\  → field separator
      \\S\\  → component separator
      \\T\\  → subcomponent separator
      \\R\\  → repetition separator
      \\E\\  → escape character itself
      \\H\\  → start highlighting (dropped)
      \\N\\  → normal text (dropped)
    """
    e = enc.esc_char
    if e not in value:
        return value
    # Process in order; \\E\\ last to avoid double-substitution issues.
    result = (
        value.replace(f"{e}F{e}", enc.field_sep)
        .replace(f"{e}S{e}", enc.comp_sep)
        .replace(f"{e}T{e}", enc.sub_comp_sep)
        .replace(f"{e}R{e}", enc.rep_sep)
        .replace(f"{e}H{e}", "")
        .replace(f"{e}N{e}", "")
        .replace(f"{e}E{e}", enc.esc_char)
    )
    return result


class HL7Segment:
    """A single HL7 segment with 1-based field/component access.

    Field numbering matches the HL7 standard:
      - For MSH: field 1 = "|" (separator), field 2 = "^~\\&", field 3 = SendingApp, …
      - For all other segments: field 1 is the first data field after the segment name.

    In both cases, split-by-field-separator produces::

        parts[0] = segment_type name
        parts[1] = field 1
        parts[2] = field 2
        …

    So ``get_field(n)`` → ``parts[n]``.
    """

    __slots__ = ("segment_type", "_fields", "_enc", "raw_line")

    def __init__(
        self,
        segment_type: str,
        fields: list[str],
        enc: HL7EncodingChars,
        raw_line: str = "",
    ) -> None:
        self.segment_type = segment_type
        self._fields = fields
        self._enc = enc
        self.raw_line = raw_line

    def get_field(self, n: int, default: str = "") -> str:
        """Return the raw value of field *n* (1-based).

        Returns *default* when *n* is out of range or the field is empty.
        """
        if n < 1 or n >= len(self._fields):
            return default
        return self._fields[n] if self._fields[n] else default

    def get_component(self, field_n: int, comp_n: int, default: str = "") -> str:
        """Return component *comp_n* (1-based) of field *field_n* (1-based).

        Splits the field on the component separator, then decodes escape sequences.
        Returns *default* when out of range or empty.
        """
        raw = self.get_field(field_n)
        if not raw:
            return default
        parts = raw.split(self._enc.comp_sep)
        if comp_n < 1 or comp_n > len(parts):
            return default
        decoded = _decode_escape(parts[comp_n - 1], self._enc)
        return decoded if decoded else default

    def get_repetition(self, field_n: int, rep_n: int = 1, default: str = "") -> str:
        """Return repetition *rep_n* (1-based) of field *field_n* (1-based).

        Most fields are non-repeating; ``rep_n=1`` returns the whole value.
        """
        raw = self.get_field(field_n)
        if not raw:
            return default
        if self._enc.rep_sep not in raw:
            return raw if rep_n == 1 else default
        parts = raw.split(self._enc.rep_sep)
        if rep_n < 1 or rep_n > len(parts):
            return default
        return parts[rep_n - 1] if parts[rep_n - 1] else default

    def get_first_repetition(self, field_n: int, default: str = "") -> str:
        """Convenience wrapper: return the first repetition of a field."""
        return self.get_repetition(field_n, rep_n=1, default=default)

    def get_rep_component(
        self, field_n: int, rep_n: int, comp_n: int, default: str = ""
    ) -> str:
        """Return component *comp_n* from repetition *rep_n* of field *field_n*.

        All indices are 1-based.
        """
        rep = self.get_repetition(field_n, rep_n)
        if not rep:
            return default
        parts = rep.split(self._enc.comp_sep)
        if comp_n < 1 or comp_n > len(parts):
            return default
        decoded = _decode_escape(parts[comp_n - 1], self._enc)
        return decoded if decoded else default

    def get_sub_component(
        self, field_n: int, comp_n: int, sub_n: int, default: str = ""
    ) -> str:
        """Return sub-component *sub_n* (1-based) of component *comp_n* of field *field_n*.

        Sub-components are separated by ``&`` (e.g. HD inside CX.4).
        """
        comp = self.get_component(field_n, comp_n)
        if not comp:
            return default
        parts = comp.split(self._enc.sub_comp_sep)
        if sub_n < 1 or sub_n > len(parts):
            return default
        decoded = _decode_escape(parts[sub_n - 1], self._enc)
        return decoded if decoded else default

    def get_rep_sub_component(
        self, field_n: int, rep_n: int, comp_n: int, sub_n: int, default: str = ""
    ) -> str:
        """Return sub-component *sub_n* of component *comp_n* from repetition
        *rep_n* of field *field_n*.

        All indices are 1-based.
        """
        comp = self.get_rep_component(field_n, rep_n, comp_n)
        if not comp:
            return default
        parts = comp.split(self._enc.sub_comp_sep)
        if sub_n < 1 or sub_n > len(parts):
            return default
        decoded = _decode_escape(parts[sub_n - 1], self._enc)
        return decoded if decoded else default

    def num_fields(self) -> int:
        """Number of data fields in this segment (excludes the segment name at index 0)."""
        return max(0, len(self._fields) - 1)


class HL7Message:
    """A parsed HL7 v2 message: an ordered list of segments."""

    __slots__ = ("segments", "enc")

    def __init__(self, segments: list[HL7Segment], enc: HL7EncodingChars) -> None:
        self.segments = segments
        self.enc = enc

    def get_segment(self, segment_type: str) -> HL7Segment | None:
        """Return the first segment of *segment_type*, or ``None``."""
        st = segment_type.upper()
        for seg in self.segments:
            if seg.segment_type == st:
                return seg
        return None

    def get_segments(self, segment_type: str) -> list[HL7Segment]:
        """Return all segments of *segment_type* (e.g., multiple OBX rows)."""
        st = segment_type.upper()
        return [seg for seg in self.segments if seg.segment_type == st]


def parse_message(text: str) -> HL7Message | None:
    """Parse raw HL7 v2 text into an :class:`HL7Message`.

    Handles both ``\\r`` and ``\\n`` line endings.  Returns ``None`` if no
    ``MSH`` segment is found.

    Args:
        text: Raw HL7 v2 message string (may contain one or many segments).

    Returns:
        An :class:`HL7Message`, or ``None`` if the input is not a valid HL7 message.
    """
    if not text or not text.strip():
        return None

    # Normalise line endings: \r\n → \r, bare \n → \r, then split on \r.
    normalised = text.strip().replace("\r\n", "\r").replace("\n", "\r")
    raw_lines = [line for line in normalised.split("\r") if line.strip()]

    if not raw_lines:
        return None

    # Locate the MSH segment to extract encoding characters.
    msh_line = next((line for line in raw_lines if line.upper().startswith("MSH")), None)
    if msh_line is None:
        return None

    # MSH structure: MSH<FS><ENC>…
    # Position 3 is the field separator; positions 4–7 are the four encoding chars.
    if len(msh_line) < 4:
        return None

    field_sep = msh_line[3]

    enc_raw = msh_line[4:8] if len(msh_line) >= 8 else "^~\\&"
    comp_sep = enc_raw[0] if len(enc_raw) > 0 else "^"
    rep_sep = enc_raw[1] if len(enc_raw) > 1 else "~"
    esc_char = enc_raw[2] if len(enc_raw) > 2 else "\\"
    sub_comp_sep = enc_raw[3] if len(enc_raw) > 3 else "&"

    enc = HL7EncodingChars(field_sep, comp_sep, rep_sep, esc_char, sub_comp_sep)

    segments: list[HL7Segment] = []
    for line in raw_lines:
        if not line:
            continue
        parts = line.split(field_sep)
        seg_type = parts[0].upper().strip()
        if not seg_type:
            continue
        if seg_type == "MSH":
            # HL7 standard: MSH-1 = "|" (the separator itself, implicit in the wire format).
            # After splitting by "|", parts[1] = "^~\&" (MSH-2), parts[2] = SendingApp (MSH-3).
            # Insert the field separator at index 1 so that get_field(N) == MSH-N for all N >= 1.
            parts.insert(1, field_sep)
        segments.append(HL7Segment(seg_type, parts, enc, line))

    if not segments:
        return None

    return HL7Message(segments, enc)
