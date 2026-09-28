"""Quote values that SQL can't take as bound parameters."""


def sql_literal(value: str) -> str:
    """Quote ``value`` as an SQL string literal (e.g. a ``read_parquet`` path)."""
    return "'" + value.replace("'", "''") + "'"


def quote_ident(name: str) -> str:
    """Quote an SQL identifier (table/column) so it can't break out of context.

    Identifiers are interpolated as text (they can't be bound as ``?``
    parameters), so they go through here. Quoting also makes the identifier
    case-sensitive, so the name must match the stored column's case.
    """
    return '"' + str(name).replace('"', '""') + '"'
