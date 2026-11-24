from dataclasses import dataclass

from lake.ledger.revisions import OfficialReceipt, validate_official_receipt
from lake.query.scope import InvalidMirrorRequest, normalize_partitions


@dataclass(frozen=True)
class IntervalRequest:
    start: OfficialReceipt
    end: OfficialReceipt
    partitions: tuple[str, ...]


def normalize_interval_request(start, end, partitions):
    try:
        start = validate_official_receipt(start)
        end = validate_official_receipt(end)
        if (start.lake_id, start.day) != (end.lake_id, end.day):
            raise ValueError('interval endpoints belong to different books')
        if start.revision >= end.revision:
            raise ValueError('interval start must precede its end')
        scope = normalize_partitions(partitions)
        return IntervalRequest(start, end, scope)
    except ValueError as exc:
        raise InvalidMirrorRequest(str(exc)) from exc
