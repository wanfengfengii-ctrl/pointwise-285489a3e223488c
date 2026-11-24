import json
import sqlite3

from lake.database import open_database
from lake.dispatch.catalog import initialize_catalog
from lake.dispatch.interval import decode_interval_package
from lake.dispatch.publish import PublicationRejected, publish, publish_interval
from lake.intake.batch import submit
from lake.intake.source_rows import ObservationKey
from lake.intake.store import initialize_intake
from lake.ledger.commit import post
from lake.ledger.corrections import correct
from lake.ledger.revisions import OfficialReceipt, initialize_ledger
from lake.query.read import mirror_interval, mirror_read
from lake.query.scope import InvalidMirrorRequest
from lake.receiving.accept import receive
from lake.receiving.receipt import PackageConflict, PackageRejected
from lake.receiving.store import open_mirror
from tests.support import ProjectCase, enter, observation, row_values, sample

LEGACY_DAY = '2025-08-14'
LEGACY_LAKE = 'survey-legacy'


class TestInterval(ProjectCase):
    def build_sample_book(self):
        data = sample('interval.json')
        daily = sample('daily.json')
        submit(self.source, daily['batch_id'], daily['rows'])
        receipts = {1: post(self.source, daily['batch_id'])}
        for step in data['corrections']:
            change = step['change']
            kwargs = {}
            if 'partition' in change:
                kwargs['partition'] = change['partition']
            if 'mm' in change:
                kwargs['mm'] = change['mm']
            receipt = correct(self.source, step['id'], step['parent'],
                              ObservationKey(*step['target']), **kwargs)
            receipts[receipt.revision] = receipt
        ordinary = data['ordinary_post']
        submit(self.source, ordinary['batch_id'], ordinary['rows'])
        receipt = post(self.source, ordinary['batch_id'])
        receipts[receipt.revision] = receipt
        return data, receipts

    def interval_piece(self, start, end, partition):
        return publish_interval(self.source, start, end, [partition])[partition]

    def test_sample_walkthrough(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        pieces = {tuple(delivery): publish_interval(
            self.source, receipts[delivery[0]], receipts[delivery[1]], [delivery[2]])[delivery[2]]
            for delivery in data['deliveries']}

        mirror = self.open_mirror()
        # Only the slice pieces for revisions 1 and 4: still short for the interval.
        receive(mirror, publish(self.source, receipts[1]).part('east'))
        receive(mirror, publish(self.source, receipts[4]).part('west'))
        self.assertEqual(mirror_interval(mirror, start, end, ['east', 'west']),
                         {'status': 'pending'})

        states = []
        for index, delivery in enumerate(data['deliveries'], 1):
            receipt = receive(mirror, pieces[tuple(delivery)])
            states.append((index, receipt,
                           mirror_interval(mirror, start, end, ['east'])['status'],
                           mirror_interval(mirror, start, end, ['east', 'west'])['status']))
            if index == 3:
                self.assertEqual(mirror_interval(mirror, start, end, ['east'])['status'], 'pending')
            if index == 4:
                self.assertEqual(mirror_interval(mirror, start, end, ['east'])['status'], 'ready')
                self.assertEqual(mirror_interval(mirror, start, end, ['east', 'west'])['status'],
                                 'pending')
            if index == 6:
                self.assertEqual(mirror_interval(mirror, start, end, ['east', 'west'])['status'],
                                 'ready')
        # Delivery 7 replays delivery 1 and must return its first receiving receipt.
        self.assertEqual(states[6][1], states[0][1])
        self.assertEqual(receive(mirror, pieces[(2, 4, 'west')]), states[0][1])

        result = mirror_interval(mirror, start, end, ['east', 'west'])
        self.assertEqual(result['status'], 'ready')
        self.assertEqual([event['correction_id'] for event in result['events']],
                         ['west-missing', 'back-east', 'same-reading'])
        self.assertEqual([event['revision'] for event in result['events']], [2, 3, 4])
        for event in result['events']:
            self.assertEqual(event['key'], ObservationKey('morning', 'R'))
            self.assertEqual((event['source'], event['line'], event['position']),
                             ('station-E/aug14', 4, 1))
        west_missing, back_east, same = result['events']
        self.assertEqual(west_missing['effects']['east'][0], 'out')
        self.assertEqual(west_missing['effects']['west'][0], 'in')
        self.assertEqual(west_missing['effects']['west'][2].mm, None)
        self.assertEqual(back_east['effects']['east'][0], 'in')
        self.assertEqual(back_east['effects']['west'][0], 'out')
        self.assertEqual(same['effects']['east'][0], 'equal')
        self.assertEqual(same['effects']['east'][1].mm, same['effects']['east'][2].mm)
        self.assertEqual(result['net'], {'east': (0, 0), 'west': (0, 0)})

        # Widening to (1,5] adds the ordinary post, which contributes nothing.
        wider = mirror_interval(mirror, receipts[1], receipts[5], ['east', 'west'])
        self.assertEqual([event['correction_id'] for event in wider['events']],
                         ['west-missing', 'back-east', 'same-reading'])
        self.assertEqual(wider['net'], {'east': (0, 0), 'west': (0, 0)})

    def test_adjacent_pieces_prove_coverage(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        mirror = self.open_mirror()
        # Two adjacent, non-overlapping pieces tile (1,4] exactly.
        receive(mirror, publish_interval(self.source, receipts[1], receipts[2],
                                         ['east'])['east'])
        self.assertEqual(mirror_interval(mirror, start, end, ['east']), {'status': 'pending'})
        receive(mirror, publish_interval(self.source, receipts[2], receipts[4],
                                         ['east'])['east'])
        result = mirror_interval(mirror, start, end, ['east'])
        self.assertEqual([event['correction_id'] for event in result['events']],
                         ['west-missing', 'back-east', 'same-reading'])

    def test_freeze_and_reissue(self):
        receipt = enter(self.source, 'morning', observation('A', 'east', 7))
        first = correct(self.source, 'one', receipt.revision,
                        ObservationKey('morning', 'A'), mm=9)
        frozen = publish_interval(self.source, receipt, first, ['east'])['east']
        # Later formal revisions must not disturb an already frozen range.
        later = correct(self.source, 'two', first.revision,
                        ObservationKey('morning', 'A'), partition='west', mm=11)
        self.assertEqual(publish_interval(self.source, receipt, first, ['east'])['east'], frozen)
        later_piece = publish_interval(self.source, receipt, later, ['east'])['east']
        self.assertNotEqual(later_piece, frozen)
        self.assertIn(b'"end":3', later_piece)
        # Freeze the widened (1,3] again after a source reopen; bytes repeat.
        self.source.close()
        self.source = self.open_source('source.sqlite')
        self.assertEqual(publish_interval(self.source, receipt, first, ['east'])['east'], frozen)
        mirror = self.open_mirror()
        accepted = receive(mirror, frozen)
        mirror.close()
        mirror = self.open_mirror()
        self.assertEqual(receive(mirror, frozen), accepted)
        result = mirror_interval(mirror, receipt, first, ['east'])
        self.assertEqual([event['correction_id'] for event in result['events']], ['one'])
        self.assertEqual(result['net'], {'east': (2, 0)})

    def test_overlapping_pieces_with_same_facts_are_accepted(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        mirror = self.open_mirror()
        # (1,3] and (3,5] overlap on revision 4; both agree on the common facts.
        for beginning, finish in ((1, 3), (3, 5), (2, 4), (1, 4)):
            receive(mirror, publish_interval(self.source, receipts[beginning],
                                             receipts[finish], ['east'])['east'])
        result = mirror_interval(mirror, start, end, ['east'])
        self.assertEqual([event['correction_id'] for event in result['events']],
                         ['west-missing', 'back-east', 'same-reading'])

    def test_overlap_with_different_effect_conflicts(self):
        from dataclasses import replace
        from lake.dispatch.interval import encode_interval_package
        data, receipts = self.build_sample_book()
        original = publish_interval(self.source, receipts[1], receipts[3], ['east'])['east']
        mirror = self.open_mirror()
        receive(mirror, original)
        package = decode_interval_package(original)
        entries = list(package.entries)
        effect = entries[1].effect  # revision 3, back-east: an 'in' effect with 7 mm
        entries[1] = replace(entries[1], effect=replace(
            effect, after=replace(effect.after, mm=9)))
        altered = encode_interval_package(replace(package, entries=tuple(entries)))
        with self.assertRaises(PackageConflict):
            receive(mirror, altered)
        self.assertEqual(receive(mirror, original).package_id, 'survey/2025-08-14/1-3/east')

    def test_empty_account_for_range_without_corrections(self):
        receipt = enter(self.source, 'morning', observation('A', 'east', 7),
                        observation('B', 'north', 5, line=2))
        ordinary = enter(self.source, 'afternoon', observation('C', 'east', 3, line=3))
        mirror = self.open_mirror()
        receive(mirror, self.interval_piece(receipt, ordinary, 'east'))
        self.assertEqual(mirror_interval(mirror, receipt, ordinary, ['east']),
                         {'status': 'ready', 'events': [], 'net': {'east': (0, 0)}})
        self.assertEqual(mirror_interval(mirror, receipt, ordinary, ['east', 'north']),
                         {'status': 'pending'})
        receive(mirror, self.interval_piece(receipt, ordinary, 'north'))
        result = mirror_interval(mirror, receipt, ordinary, ['east', 'north'])
        self.assertEqual(result, {'status': 'ready', 'events': [],
                                  'net': {'east': (0, 0), 'north': (0, 0)}})

    def test_interval_and_slice_share_a_mirror(self):
        receipt = enter(self.source, 'morning', observation('A', 'east', 7))
        later = correct(self.source, 'tweak', receipt.revision,
                        ObservationKey('morning', 'A'), mm=9)
        slice_release = publish(self.source, later)
        interval = self.interval_piece(receipt, later, 'east')
        mirror = self.open_mirror()
        receive(mirror, slice_release.part('east'))
        receive(mirror, interval)
        mirror.close()
        mirror = self.open_mirror()
        self.assertEqual(mirror_read(mirror, later, ['east']).totals.total_mm, 9)
        result = mirror_interval(mirror, receipt, later, ['east'])
        self.assertEqual([event['correction_id'] for event in result['events']], ['tweak'])
        self.assertEqual(result['net'], {'east': (2, 0)})

    def test_publish_contract(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        both = publish_interval(self.source, start, end, ['west', 'east', 'east'])
        self.assertEqual(list(both), ['east', 'west'])
        single = publish_interval(self.source, start, end, ['west'])
        self.assertEqual(single['west'], both['west'])
        self.assertEqual(publish_interval(self.source, start, end, ['east', 'west']), both)
        package = decode_interval_package(both['east'])
        self.assertEqual((package.start, package.end, package.partition), (1, 4, 'east'))
        # A partition with no corrections still yields a covering piece.
        empty = publish_interval(self.source, receipts[4], receipts[5], ['north'])
        self.assertEqual(decode_interval_package(empty['north']).entries[0].kind, 'post')

        with self.assertRaises(PublicationRejected):
            publish_interval(self.source, end, start, ['east'])
        with self.assertRaises(PublicationRejected):
            publish_interval(self.source, start, start, ['east'])
        with self.assertRaises(PublicationRejected):
            publish_interval(self.source, start, OfficialReceipt(start.lake_id, start.day, 99),
                             ['east'])
        with self.assertRaises(PublicationRejected):
            publish_interval(self.source, start, end, [])
        with self.assertRaises(PublicationRejected):
            publish_interval(self.source, start, end, ['bad name'])

    def test_invalid_mirror_request(self):
        receipt = enter(self.source, 'morning', observation('A', 'east', 7))
        later = enter(self.source, 'after', observation('B', 'east', 3, line=2))
        mirror = self.open_mirror()
        receive(mirror, self.interval_piece(receipt, later, 'east'))
        invalid = [
            (later, receipt, ['east']),
            (receipt, receipt, ['east']),
            (receipt, later, []),
            (receipt, later, ['bad name']),
            (receipt, OfficialReceipt('other', receipt.day, 2), ['east']),
            (receipt, later, 'east'),
        ]
        for start, end, scope in invalid:
            with self.subTest(scope=scope), self.assertRaises(InvalidMirrorRequest):
                mirror_interval(mirror, start, end, scope)
        future = OfficialReceipt(receipt.lake_id, receipt.day, 9)
        self.assertEqual(mirror_interval(mirror, later, future, ['east']), {'status': 'pending'})

    def test_bad_and_conflicting_packages(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        payload = self.interval_piece(start, end, 'east')
        mirror = self.open_mirror()
        accepted = receive(mirror, payload)
        self.assertEqual(self.interval_count(mirror), 1)

        damaged = json.loads(payload)
        damaged['body']['records'][0]['correction_id'] = 'forged'
        with self.assertRaises(PackageRejected):
            receive(mirror, json.dumps(damaged).encode('utf-8'))
        with self.assertRaises(PackageRejected):
            receive(mirror, b'not a package')
        with self.assertRaises(PackageConflict):
            forged = decode_interval_package(payload)
            from dataclasses import replace
            from lake.dispatch.interval import encode_interval_package
            entries = list(forged.entries)
            entries[0] = replace(entries[0], correction_id='other')
            receive(mirror, encode_interval_package(replace(forged, entries=tuple(entries))))
        self.assertEqual(receive(mirror, json.dumps(json.loads(payload), indent=2).encode()),
                         accepted)
        self.assertEqual(self.interval_count(mirror), 1)

    def test_receive_rollback_preserves_material(self):
        data, receipts = self.build_sample_book()
        start, end = receipts[1], receipts[4]
        mirror = self.open_mirror()
        first = receive(mirror, self.interval_piece(start, end, 'west'))
        mirror.execute('''CREATE TEMP TRIGGER stop_interval_write
            BEFORE INSERT ON mirror_intervals WHEN NEW.partition = 'east'
            BEGIN SELECT RAISE(ABORT, 'interval write stopped'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'interval write stopped'):
            receive(mirror, self.interval_piece(start, end, 'east'))
        self.assertEqual(self.interval_count(mirror), 1)
        self.assertEqual(mirror_interval(mirror, start, end, ['east', 'west']),
                         {'status': 'pending'})
        mirror.execute('DROP TRIGGER stop_interval_write')
        self.assertEqual(receive(mirror, self.interval_piece(start, end, 'east')).package_id,
                         'survey/2025-08-14/1-4/east')
        self.assertEqual(mirror_interval(mirror, start, end, ['east', 'west'])['status'], 'ready')
        self.assertEqual(receive(mirror, self.interval_piece(start, end, 'west')), first)

    def test_conflicting_cross_account_material(self):
        first = self.build_cross_account(second_to_north=False)
        second = self.build_cross_account(second_to_north=True)
        self.source.close()
        mirror = self.open_mirror('first-order.sqlite')
        east_piece = publish_interval(first[0], first[1], first[2], ['east'])['east']
        west_piece = publish_interval(second[0], second[1], second[3], ['west'])['west']
        receive(mirror, east_piece)
        with self.assertRaises(PackageConflict):
            receive(mirror, west_piece)
        self.assertEqual(self.interval_count(mirror), 1)

        reverse = self.open_mirror('reverse-order.sqlite')
        receive(reverse, west_piece)
        with self.assertRaises(PackageConflict):
            receive(reverse, east_piece)
        self.assertEqual(self.interval_count(reverse), 1)

    def build_cross_account(self, *, second_to_north):
        db = self.open_source(f'account-{second_to_north}.sqlite')
        submit(db, 'morning', [observation('R', 'east', 7, source='station-E/aug14', line=4),
                               observation('B', 'north', 5, source='station-N/aug14', line=9)])
        first = post(db, 'morning')
        destination = 'north' if second_to_north else 'west'
        second = correct(db, 'move', 1, ObservationKey('morning', 'R'),
                         partition=destination, mm=11)
        third = correct(db, 'back', 2, ObservationKey('morning', 'R'),
                        partition='east', mm=7)
        return db, first, second, third

    @staticmethod
    def interval_count(mirror):
        return mirror.execute('SELECT COUNT(*) FROM mirror_intervals').fetchone()[0]

    # -- legacy account -----------------------------------------------------

    def open_legacy_book(self):
        raw = sqlite3.connect(self.directory / 'legacy.sqlite')
        raw.executescript(sample_text('interval-v1.sql'))
        raw.commit()
        raw.close()
        db = open_database(self.directory / 'legacy.sqlite')
        self.addCleanup(db.close)
        initialize_intake(db)
        initialize_ledger(db, LEGACY_LAKE, LEGACY_DAY)
        initialize_catalog(db)
        return db

    def test_legacy_unresolvable_and_new_retest(self):
        source = self.open_legacy_book()
        start = OfficialReceipt(LEGACY_LAKE, LEGACY_DAY, 1)
        end = OfficialReceipt(LEGACY_LAKE, LEGACY_DAY, 3)
        pieces = publish_interval(source, start, end, ['east', 'north'])
        mirror = self.open_mirror('legacy-mirror.sqlite')
        receive(mirror, pieces['east'])
        receive(mirror, pieces['north'])
        self.assertEqual(mirror_interval(mirror, start, end, ['east']),
                         {'status': 'unresolvable'})
        north = mirror_interval(mirror, start, end, ['north'])
        self.assertEqual(north['status'], 'ready')
        self.assertEqual([(event['revision'], event['correction_id'], event['key'])
                          for event in north['events']],
                         [(3, 'same-north', ObservationKey('early', 'B'))])
        self.assertEqual(north['net'], {'north': (0, 0)})

        # A newly issued equal-value retest keeps its actual target.
        retest = correct(source, 'S-retest', 3, ObservationKey('early', 'S'), mm=7)
        piece = publish_interval(source, end, retest, ['east'])['east']
        retest_mirror = self.open_mirror('retest-mirror.sqlite')
        receive(retest_mirror, piece)
        result = mirror_interval(retest_mirror, end, retest, ['east'])
        self.assertEqual([(event['correction_id'], event['key'],
                           event['effects']['east'][0]) for event in result['events']],
                         [('S-retest', ObservationKey('early', 'S'), 'equal')])
        self.assertEqual(result['net'], {'east': (0, 0)})

    def test_legacy_equal_correction_is_not_unresolvable_when_out_of_scope(self):
        source = self.open_legacy_book()
        start = OfficialReceipt(LEGACY_LAKE, LEGACY_DAY, 1)
        end = OfficialReceipt(LEGACY_LAKE, LEGACY_DAY, 3)
        mirror = self.open_mirror('legacy-scope-mirror.sqlite')
        receive(mirror, publish_interval(source, start, end, ['north'])['north'])
        # North alone is complete and its own correction resolves uniquely.
        self.assertEqual(mirror_interval(mirror, start, end, ['north'])['status'], 'ready')


def sample_text(name):
    from pathlib import Path
    return (Path(__file__).resolve().parents[1] / 'samples' / name).read_text(encoding='utf-8')