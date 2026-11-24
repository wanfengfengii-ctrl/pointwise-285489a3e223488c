from lake.database import atomic
from lake.dispatch.catalog import (
    ReleaseRecord, find_interval, find_release, save_interval, save_release,
)
from lake.dispatch.interval import (
    IntervalFormatError, build_interval_package, encode_interval_package,
    interval_identifier,
)
from lake.dispatch.manifest import build_manifest
from lake.dispatch.partition_package import build_partition_package, release_identifier
from lake.dispatch.wire import encode_package
from lake.ledger.history import read_release_basis
from lake.ledger.revisions import OfficialReceipt, resolve_official_receipt
from lake.query.scope import normalize_partitions


class PublicationRejected(ValueError):
    pass


def publish(db, receipt):
    with atomic(db):
        if resolve_official_receipt(db, receipt) is None:
            raise PublicationRejected('unknown official receipt for this observation book')
        stored = find_release(db, receipt)
        if stored is not None:
            return stored
        basis = read_release_basis(db, receipt)
        manifest = build_manifest(basis)
        parts = []
        for entry in manifest.coverage:
            package = build_partition_package(manifest, entry.partition, basis.rows)
            parts.append((entry.partition, encode_package(package)))
        release = ReleaseRecord(release_identifier(receipt), receipt, tuple(parts))
        save_release(db, release)
        return release


def _validate_interval_endpoint(db, endpoint, label):
    if type(endpoint) is not OfficialReceipt:
        raise PublicationRejected(f'{label} is not an official receipt')
    if resolve_official_receipt(db, endpoint) is None:
        raise PublicationRejected(f'{label} is not a known formal revision')
    return endpoint


def publish_interval(db, start, end, partitions):
    try:
        scope = normalize_partitions(partitions)
    except ValueError as exc:
        raise PublicationRejected(str(exc)) from exc
    with atomic(db):
        start = _validate_interval_endpoint(db, start, 'interval start')
        end = _validate_interval_endpoint(db, end, 'interval end')
        if (start.lake_id, start.day) != (end.lake_id, end.day):
            raise PublicationRejected('interval endpoints belong to different books')
        if start.revision >= end.revision:
            raise PublicationRejected('interval start must precede its end')
        parts = {}
        for partition in scope:
            identifier = interval_identifier(start.lake_id, start.day,
                                             start.revision, end.revision, partition)
            stored = find_interval(db, identifier)
            if stored is not None:
                parts[partition] = stored
                continue
            try:
                package = build_interval_package(db, start, end, partition)
            except IntervalFormatError as exc:
                raise PublicationRejected(str(exc)) from exc
            payload = encode_interval_package(package)
            save_interval(db, identifier, start.lake_id, start.day,
                          start.revision, end.revision, partition, payload)
            parts[partition] = payload
        return {name: parts[name] for name in sorted(parts)}
