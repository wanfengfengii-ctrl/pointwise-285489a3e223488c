"""Fixed-interval correction packages: build, canonical wire form, strict decode.

An interval piece binds one lake/day, one partition and the exact revision range
``(start, end]``.  For every formal revision in that range it records the common
facts of the operation (a plain post or a correction), the partitions the
operation touched, and -- when this partition is hit -- the effect on it.  The
same piece is independently transportable and can be combined with adjacent or
overlapping pieces to prove coverage.
"""
from dataclasses import dataclass
import hashlib
import json

from lake.intake.source_rows import ObservationKey, decode_mm, encode_mm
from lake.ledger.revisions import (
    OfficialRow, read_correction_target, read_revision, read_revision_rows,
)


class IntervalFormatError(ValueError):
    pass


@dataclass(frozen=True)
class IntervalEffect:
    kind: str
    before: OfficialRow | None
    after: OfficialRow | None


@dataclass(frozen=True)
class IntervalEntry:
    revision: int
    kind: str                       # 'post' or 'correction'
    partitions: tuple[str, ...]     # partitions touched by the operation
    correction_id: str | None
    target_state: str | None        # 'known' or 'ambiguous' for corrections
    key: ObservationKey | None
    source: str | None
    line: int | None
    position: int | None
    effect: IntervalEffect | None   # effect on this piece's partition, if hit


@dataclass(frozen=True)
class IntervalPackage:
    package_id: str
    lake_id: str
    day: str
    start: int
    end: int
    partition: str
    entries: tuple[IntervalEntry, ...]

    def entry_at(self, revision):
        index = revision - self.start - 1
        if 0 <= index < len(self.entries) and self.entries[index].revision == revision:
            return self.entries[index]
        return None


def interval_identifier(lake_id, day, start, end, partition):
    return f'{lake_id}/{day}/{start}-{end}/{partition}'


# ---------------------------------------------------------------------------
# canonical encoding


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def _digest(body):
    return hashlib.sha256(_canonical(body)).hexdigest()


def _encode_row(row):
    return {
        'batch_id': row.key.batch_id,
        'observation': row.key.observation,
        'partition': row.partition,
        'source': row.source,
        'line': row.line,
        'position': row.position,
        'mm': encode_mm(row.mm),
    }


def _encode_effect(effect):
    if effect is None:
        return None
    return {
        'kind': effect.kind,
        'before': None if effect.before is None else _encode_row(effect.before),
        'after': None if effect.after is None else _encode_row(effect.after),
    }


def _encode_entry(entry):
    return {
        'revision': entry.revision,
        'kind': entry.kind,
        'partitions': list(entry.partitions),
        'correction_id': entry.correction_id,
        'target_state': entry.target_state,
        'key': None if entry.key is None else {
            'batch_id': entry.key.batch_id, 'observation': entry.key.observation},
        'source': entry.source,
        'line': entry.line,
        'position': entry.position,
        'effect': _encode_effect(entry.effect),
    }


def encode_interval_package(package):
    body = {
        'package_type': 'interval',
        'package_id': package.package_id,
        'lake_id': package.lake_id,
        'day': package.day,
        'start': package.start,
        'end': package.end,
        'partition': package.partition,
        'records': [_encode_entry(entry) for entry in package.entries],
    }
    return _canonical({'version': 1, 'sha256': _digest(body), 'body': body})


# ---------------------------------------------------------------------------
# strict decoding


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise IntervalFormatError(f'duplicate object key: {key}')
        result[key] = value
    return result


def _fields(value, names):
    if type(value) is not dict or value.keys() != names:
        raise IntervalFormatError('unexpected object fields')
    return value


def _array(value):
    if type(value) is not list:
        raise IntervalFormatError('expected an array')
    return value


def _decode_row(data):
    _fields(data, {'batch_id', 'observation', 'partition', 'source', 'line',
                   'position', 'mm'})
    return OfficialRow(ObservationKey(data['batch_id'], data['observation']),
                       data['partition'], data['source'], data['line'],
                       decode_mm(data['mm']), data['position'])


def _decode_effect(data):
    if data is None:
        return None
    _fields(data, {'kind', 'before', 'after'})
    before = None if data['before'] is None else _decode_row(data['before'])
    after = None if data['after'] is None else _decode_row(data['after'])
    return IntervalEffect(data['kind'], before, after)


def _decode_entry(data):
    _fields(data, {'revision', 'kind', 'partitions', 'correction_id',
                   'target_state', 'key', 'source', 'line', 'position', 'effect'})
    key = data['key']
    if key is not None:
        _fields(key, {'batch_id', 'observation'})
        key = ObservationKey(key['batch_id'], key['observation'])
    return IntervalEntry(data['revision'], data['kind'], tuple(_array(data['partitions'])),
                         data['correction_id'], data['target_state'], key,
                         data['source'], data['line'], data['position'],
                         _decode_effect(data['effect']))


def decode_interval_package(payload):
    if type(payload) is not bytes:
        raise IntervalFormatError('a package must be transported as bytes')
    try:
        envelope = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique_object)
        _fields(envelope, {'version', 'sha256', 'body'})
        if type(envelope['version']) is not int or envelope['version'] != 1:
            raise IntervalFormatError('unsupported package version')
        body = _fields(envelope['body'], {'package_type', 'package_id', 'lake_id', 'day',
                                         'start', 'end', 'partition', 'records'})
        if body['package_type'] != 'interval':
            raise IntervalFormatError('not an interval package')
        if envelope['sha256'] != _digest(body):
            raise IntervalFormatError('package body digest mismatch')
        return IntervalPackage(body['package_id'], body['lake_id'], body['day'],
                               body['start'], body['end'], body['partition'],
                               tuple(_decode_entry(raw) for raw in _array(body['records'])))
    except IntervalFormatError:
        raise
    except (ValueError, TypeError, RecursionError) as exc:
        raise IntervalFormatError('invalid package encoding') from exc


def payload_kind(payload):
    """Best-effort envelope peek; used only to route a payload to its decoder."""
    if type(payload) is not bytes:
        return None
    try:
        envelope = json.loads(payload.decode('utf-8'))
        body = envelope.get('body') if type(envelope) is dict else None
        return body.get('package_type') if type(body) is dict else None
    except (ValueError, TypeError, RecursionError):
        return None


# ---------------------------------------------------------------------------
# source-side building


def _same_row(left, right):
    return left.partition == right.partition and left.mm == right.mm


def _infer_target(parent_rows, child_rows, affected):
    """Recover a legacy correction's target only when the old account pins it."""
    before = {row.key: row for row in parent_rows}
    after = {row.key: row for row in child_rows}
    if set(before) != set(after):
        return None
    changed = [key for key in before if not _same_row(before[key], after[key])]
    scope = set(affected)
    if len(changed) > 1:
        return None
    if len(changed) == 1:
        key = changed[0]
        if {before[key].partition, after[key].partition} <= scope:
            return key
        return None
    candidates = [key for key, row in before.items()
                  if row.partition in scope and _same_row(row, after[key])]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _effect_for(partition, before, after):
    """Project one observation onto ``partition`` before and after a correction."""
    side_before = before if before is not None and before.partition == partition else None
    side_after = after if after is not None and after.partition == partition else None
    if side_before is not None and side_after is not None:
        kind = 'equal' if side_before.mm == side_after.mm else 'value'
        return IntervalEffect(kind, side_before, side_after)
    if side_before is not None:
        return IntervalEffect('out', side_before, None)
    if side_after is not None:
        return IntervalEffect('in', None, side_after)
    return None


def _build_entry(db, official, partition):
    number = official.receipt.revision
    if official.operation_kind == 'post':
        return IntervalEntry(number, 'post', official.affected, None, None,
                             None, None, None, None, None)
    parent_rows = () if official.parent is None else read_revision_rows(db, official.parent)
    child_rows = read_revision_rows(db, number)
    before = {row.key: row for row in parent_rows}
    after = {row.key: row for row in child_rows}

    explicit = read_correction_target(db, number)
    if explicit is not None:
        key, state = explicit, 'known'
    else:
        # The target is a common fact shared by every piece that records this
        # revision, so it is judged from the old account alone, never per piece.
        key = _infer_target(parent_rows, child_rows, official.affected)
        state = 'known' if key is not None else 'ambiguous'

    if key is None:
        return IntervalEntry(number, 'correction', official.affected,
                             official.operation_id, state, None,
                             None, None, None, None)

    target_before = before.get(key)
    target_after = after.get(key)
    effect = _effect_for(partition, target_before, target_after)
    anchor = target_after if target_after is not None else target_before
    return IntervalEntry(number, 'correction', official.affected,
                         official.operation_id, state, key,
                         anchor.source, anchor.line, anchor.position, effect)


def build_interval_package(db, start, end, partition):
    entries = []
    for number in range(start.revision + 1, end.revision + 1):
        official = read_revision(db, number)
        if official is None:
            raise IntervalFormatError(f'missing formal revision {number}')
        entries.append(_build_entry(db, official, partition))
    identifier = interval_identifier(start.lake_id, start.day, start.revision,
                                     end.revision, partition)
    return IntervalPackage(identifier, start.lake_id, start.day, start.revision,
                           end.revision, partition, tuple(entries))
