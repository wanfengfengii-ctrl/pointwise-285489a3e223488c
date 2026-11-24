"""Offline interval reconciliation over the fixed-interval pieces in a mirror.

``mirror_interval`` reads ``(start, end]`` for the requested partitions using the
interval pieces already received.  It never consults the source lake.  Coverage
may be assembled from adjacent, overlapping, or endpoint-overshooting pieces.
"""
from dataclasses import dataclass

from lake.dispatch.interval import IntervalPackage
from lake.intake.source_rows import ObservationKey
from lake.query.interval_scope import normalize_interval_request
from lake.receiving.interval import read_book_intervals
from lake.receiving.store import mirror_read_transaction


@dataclass(frozen=True)
class IntervalEvent:
    revision: int
    correction_id: str
    key: ObservationKey
    source: str
    line: int
    position: int
    effects: dict


def _covers(package: IntervalPackage, revision):
    return package.start < revision <= package.end


def _coverage(book, request):
    """Return {partition: {revision: covering piece}} or None when under-covered."""
    covered = {}
    for partition in request.partitions:
        by_revision = {}
        for package in book:
            if package.partition != partition:
                continue
            for revision in range(max(package.start, request.start.revision) + 1,
                                  min(package.end, request.end.revision) + 1):
                by_revision.setdefault(revision, package)
        wanted = range(request.start.revision + 1, request.end.revision + 1)
        if any(revision not in by_revision for revision in wanted):
            return None
        covered[partition] = by_revision
    return covered


def _entry_for(book, partition, revision):
    for package in book:
        if package.partition == partition and _covers(package, revision):
            entry = package.entry_at(revision)
            if entry is not None:
                return entry
    return None


_UNRESOLVABLE = object()


def _collect_events(book, request, covered):
    scope = set(request.partitions)
    events = []
    for revision in range(request.start.revision + 1, request.end.revision + 1):
        # Common facts are identical across pieces (mismatched pieces are
        # rejected at receive time), so any covering piece's record speaks for
        # the whole revision.
        reference = _entry_for(book, request.partitions[0], revision)
        if reference is None or reference.kind != 'correction':
            continue
        if reference.target_state == 'ambiguous' and scope & set(reference.partitions):
            return _UNRESOLVABLE
        if reference.target_state != 'known':
            continue
        effects = {}
        for partition in request.partitions:
            if partition not in reference.partitions:
                continue
            entry = _entry_for(book, partition, revision)
            if entry is None or entry.effect is None:
                continue
            effects[partition] = (entry.effect.kind, entry.effect.before, entry.effect.after)
        if not effects:
            continue
        events.append(IntervalEvent(revision, reference.correction_id, reference.key,
                                    reference.source, reference.line,
                                    reference.position, effects))
    return events


def _net(events, partitions):
    net = {}
    for partition in partitions:
        mm = 0
        missing = 0
        for event in events:
            effect = event.effects.get(partition)
            if effect is None:
                continue
            _, before, after = effect
            if before is not None:
                if before.mm is None:
                    missing -= 1
                else:
                    mm -= before.mm
            if after is not None:
                if after.mm is None:
                    missing += 1
                else:
                    mm += after.mm
        net[partition] = (mm, missing)
    return net


def mirror_interval(db, start, end, partitions):
    request = normalize_interval_request(start, end, partitions)
    with mirror_read_transaction(db):
        book = read_book_intervals(db, request.start.lake_id, request.start.day)
        covered = _coverage(book, request)
        if covered is None:
            return {'status': 'pending'}
        events = _collect_events(book, request, covered)
        if events is _UNRESOLVABLE:
            return {'status': 'unresolvable'}
        return {
            'status': 'ready',
            'events': [
                {'revision': event.revision,
                 'correction_id': event.correction_id,
                 'key': event.key,
                 'source': event.source,
                 'line': event.line,
                 'position': event.position,
                 'effects': event.effects}
                for event in events
            ],
            'net': _net(events, request.partitions),
        }
