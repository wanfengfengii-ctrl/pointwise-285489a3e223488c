"""Receiving fixed-interval pieces into a mirror.

Interval pieces are a distinct package type from the slice projections in
``lake.receiving``; they share the mirror but keep their own inventory so a
mirror can hold, reopen and audit both.  Pieces may arrive out of order, with
gaps, overlapping, or replayed.  Replays of the same identity return the first
receiving receipt; the same identity with different content, or overlapping
material that disagrees on a formal revision, is a conflict.
"""
from lake.dispatch.interval import (
    IntervalPackage, decode_interval_package, encode_interval_package,
    interval_identifier,
)
from lake.intake.validate import (
    validate_day, validate_mm, validate_name, validate_positive_id, validate_source,
)
from lake.receiving.receipt import PackageConflict, PackageRejected, ReceivingReceipt
from lake.receiving.store import receiving_transaction


def _check_row(row, partition, key):
    validate_name(row.key.batch_id, 'row batch')
    validate_name(row.key.observation, 'row observation')
    validate_source(row.source)
    validate_positive_id(row.line, 'source line')
    validate_mm(row.mm)
    validate_positive_id(row.position, 'formal row position')
    if row.partition != partition:
        raise ValueError('effect row escapes the partition projection')
    if row.key != key:
        raise ValueError('effect row identity differs from the correction target')


def _check_effect(effect, partition, key):
    if effect.kind not in ('out', 'in', 'value', 'equal'):
        raise ValueError('unknown effect kind')
    if effect.kind == 'out':
        if effect.before is None or effect.after is not None:
            raise ValueError('out effect must remove a row')
        _check_row(effect.before, partition, key)
    elif effect.kind == 'in':
        if effect.before is not None or effect.after is None:
            raise ValueError('in effect must add a row')
        _check_row(effect.after, partition, key)
    else:
        if effect.before is None or effect.after is None:
            raise ValueError(f'{effect.kind} effect needs both rows')
        _check_row(effect.before, partition, key)
        _check_row(effect.after, partition, key)


def _check_entry(entry, partition, revision):
    if entry.revision != revision:
        raise ValueError('interval records must span the exact revision range')
    validate_positive_id(entry.revision, 'entry revision')
    if entry.kind not in ('post', 'correction'):
        raise ValueError('unknown operation kind')
    if not entry.partitions:
        raise ValueError('an operation must touch at least one partition')
    previous = None
    for name in entry.partitions:
        validate_name(name, 'affected partition')
        if previous is not None and name <= previous:
            raise ValueError('affected partitions must be sorted and unique')
        previous = name

    if entry.kind == 'post':
        if (entry.correction_id is not None or entry.target_state is not None
                or entry.key is not None or entry.source is not None
                or entry.line is not None or entry.position is not None
                or entry.effect is not None):
            raise ValueError('ordinary posts carry no correction detail')
        return

    validate_name(entry.correction_id, 'correction id')
    if entry.target_state == 'ambiguous':
        if (entry.key is not None or entry.effect is not None
                or entry.source is not None or entry.line is not None
                or entry.position is not None):
            raise ValueError('an ambiguous target carries no row detail')
        return
    if entry.target_state != 'known':
        raise ValueError('unknown target state')
    if entry.key is None:
        raise ValueError('a known target needs an observation key')
    validate_positive_id(entry.line, 'correction source line')
    validate_positive_id(entry.position, 'correction position')
    validate_source(entry.source)
    validate_name(entry.key.batch_id, 'target batch')
    validate_name(entry.key.observation, 'target observation')
    if partition in entry.partitions:
        if entry.effect is None:
            raise ValueError('an affected partition needs an effect')
        _check_effect(entry.effect, partition, entry.key)
    elif entry.effect is not None:
        raise ValueError('an unaffected partition has no effect')


def _check_package(package):
    validate_name(package.lake_id, 'lake id')
    validate_day(package.day)
    validate_positive_id(package.start, 'interval start')
    validate_positive_id(package.end, 'interval end')
    if package.start >= package.end:
        raise ValueError('interval start must precede its end')
    validate_name(package.partition, 'package partition')
    expected = interval_identifier(package.lake_id, package.day,
                                   package.start, package.end, package.partition)
    if package.package_id != expected:
        raise ValueError('package identity does not match its range and partition')
    revisions = range(package.start + 1, package.end + 1)
    if len(package.entries) != len(revisions):
        raise ValueError('interval records do not fill the declared range')
    for entry, revision in zip(package.entries, revisions):
        _check_entry(entry, package.partition, revision)


def check_interval_package(payload):
    try:
        package = decode_interval_package(payload)
        _check_package(package)
        return package
    except ValueError as exc:
        raise PackageRejected(str(exc)) from exc


def _facts(entry):
    return (entry.kind, entry.correction_id, entry.partitions, entry.target_state,
            entry.key, entry.source, entry.line, entry.position)


def _conflicts(existing: IntervalPackage, incoming: IntervalPackage):
    if (existing.lake_id, existing.day) != (incoming.lake_id, incoming.day):
        return False
    low = max(existing.start, incoming.start)
    high = min(existing.end, incoming.end)
    for revision in range(low + 1, high + 1):
        left = existing.entry_at(revision)
        right = incoming.entry_at(revision)
        if left is None or right is None:
            continue
        if _facts(left) != _facts(right):
            return True
        if existing.partition == incoming.partition and left.effect != right.effect:
            return True
    return False


def find_interval(db, package_id):
    record = db.execute('''SELECT receiving_id, payload FROM mirror_intervals
        WHERE package_id = ?''', (package_id,)).fetchone()
    if record is None:
        return None
    return ReceivingReceipt(record[0], package_id), record[1]


def _read_book_intervals(db, lake_id, day):
    records = db.execute('''SELECT payload FROM mirror_intervals
        WHERE lake_id = ? AND day = ? ORDER BY package_id''', (lake_id, day))
    return tuple(decode_interval_package(payload) for (payload,) in records)


def read_book_intervals(db, lake_id, day):
    return _read_book_intervals(db, lake_id, day)


def receive_interval(db, payload):
    package = check_interval_package(payload)
    sealed = encode_interval_package(package)
    with receiving_transaction(db):
        stored = find_interval(db, package.package_id)
        if stored is not None:
            receipt, previous = stored
            if previous != sealed:
                raise PackageConflict('a different package already has this identity')
            return receipt
        for existing in _read_book_intervals(db, package.lake_id, package.day):
            if _conflicts(existing, package):
                raise PackageConflict('received material disagrees on a formal revision')
        insert = db.execute('''INSERT INTO mirror_intervals
            (package_id, lake_id, day, start_revision, end_revision, partition, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
                            (package.package_id, package.lake_id, package.day,
                             package.start, package.end, package.partition, sealed))
        return ReceivingReceipt(insert.lastrowid, package.package_id)
