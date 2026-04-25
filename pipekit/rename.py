"""Field renaming utilities for pipeline records."""

from typing import Callable, Dict, List, Union


def rename_fields(
    mapping: Dict[str, str],
    *,
    strict: bool = False,
) -> Callable[[List[dict]], List[dict]]:
    """
    Return a step that renames fields in every record according to *mapping*.

    Parameters
    ----------
    mapping:
        ``{old_name: new_name}`` pairs.
    strict:
        If ``True``, raise ``KeyError`` when a source field is absent from a
        record.  If ``False`` (default) the record is left unchanged for that
        field.
    """
    def transform(records: List[dict]) -> List[dict]:
        out = []
        for record in records:
            new_record = dict(record)
            for old, new in mapping.items():
                if old in new_record:
                    new_record[new] = new_record.pop(old)
                elif strict:
                    raise KeyError(
                        f"rename_fields: field '{old}' not found in record"
                    )
            out.append(new_record)
        return out

    transform.__name__ = "rename_fields"
    return transform


def prefix_fields(
    prefix: str,
    *,
    exclude: Union[List[str], None] = None,
) -> Callable[[List[dict]], List[dict]]:
    """
    Return a step that prepends *prefix* to every field name.

    Parameters
    ----------
    prefix:
        String to prepend.
    exclude:
        Field names that should not be renamed.
    """
    _exclude = set(exclude or [])

    def transform(records: List[dict]) -> List[dict]:
        out = []
        for record in records:
            new_record = {
                (k if k in _exclude else f"{prefix}{k}"): v
                for k, v in record.items()
            }
            out.append(new_record)
        return out

    transform.__name__ = "prefix_fields"
    return transform


def suffix_fields(
    suffix: str,
    *,
    exclude: Union[List[str], None] = None,
) -> Callable[[List[dict]], List[dict]]:
    """
    Return a step that appends *suffix* to every field name.

    Parameters
    ----------
    suffix:
        String to append.
    exclude:
        Field names that should not be renamed.
    """
    _exclude = set(exclude or [])

    def transform(records: List[dict]) -> List[dict]:
        out = []
        for record in records:
            new_record = {
                (k if k in _exclude else f"{k}{suffix}"): v
                for k, v in record.items()
            }
            out.append(new_record)
        return out

    transform.__name__ = "suffix_fields"
    return transform
