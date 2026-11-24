"""Interval hand-off demonstration.

Rebuilds the ``samples/interval.json`` book, issues the exact fixed-interval
pieces listed in its ``deliveries``, closes the source lake, and drives the
mirror through short delivery, gap-filling, replay and widening.  Prints one
``POINTWISE_SCENARIO`` line with the observed results and their oracle.
"""
from contextlib import closing
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake.database import open_database
from lake.dispatch.catalog import initialize_catalog
from lake.dispatch.publish import publish, publish_interval
from lake.intake.batch import submit
from lake.intake.source_rows import ObservationKey
from lake.intake.store import initialize_intake
from lake.ledger.commit import post
from lake.ledger.corrections import correct
from lake.ledger.revisions import initialize_ledger
from lake.query.read import mirror_interval
from lake.receiving.accept import receive
from lake.receiving.store import open_mirror


def _shape(result):
    if result['status'] != 'ready':
        return {'status': result['status']}
    return {
        'status': result['status'],
        'events': [{
            'revision': event['revision'],
            'correction_id': event['correction_id'],
            'observation': event['key'].observation,
            'source': event['source'],
            'line': event['line'],
            'position': event['position'],
            'effects': {partition: [kind,
                                    None if before is None else before.mm,
                                    None if after is None else after.mm]
                        for partition, (kind, before, after)
                        in sorted(event['effects'].items())},
        } for event in result['events']],
        'net': {partition: list(values) for partition, values in sorted(result['net'].items())},
    }


def run_interval_demo(samples, directory):
    data = json.loads((samples / 'interval.json').read_text(encoding='utf-8'))
    daily = json.loads((samples / 'daily.json').read_text(encoding='utf-8'))

    with closing(open_database(directory / 'source.sqlite')) as source:
        initialize_intake(source)
        initialize_ledger(source, daily['lake_id'], daily['day'])
        initialize_catalog(source)
        submit(source, daily['batch_id'], daily['rows'])
        receipts = {1: post(source, daily['batch_id'])}
        for step in data['corrections']:
            change = step['change']
            opts = {key: change[key] for key in ('partition', 'mm') if key in change}
            receipts[receipts.__len__() + 1] = correct(
                source, step['id'], step['parent'],
                ObservationKey(*step['target']), **opts)
        ordinary = data['ordinary_post']
        submit(source, ordinary['batch_id'], ordinary['rows'])
        receipts[receipts.__len__() + 1] = post(source, ordinary['batch_id'])

        start, end = receipts[1], receipts[4]
        pieces = {
            tuple(delivery): publish_interval(source, receipts[delivery[0]],
                                              receipts[delivery[1]], [delivery[2]])[delivery[2]]
            for delivery in data['deliveries']
        }
        # A slice piece proves only its own projection, not the interval ledger.
        early_slice = publish(source, receipts[1])
        late_slice = publish(source, receipts[4])

    mirror_path = directory / 'mirror.sqlite'
    trace = {'slice_only': None, 'deliveries': [], 'replay': None,
             'closed': None, 'widened': None}

    with closing(open_mirror(mirror_path)) as mirror:
        receive(mirror, early_slice.part('east'))
        receive(mirror, late_slice.part('west'))
        trace['slice_only'] = _shape(mirror_interval(mirror, start, end, ['east', 'west']))

        for index, delivery in enumerate(data['deliveries'], 1):
            snapshot = pieces[tuple(delivery)]
            accepted = receive(mirror, snapshot)
            if index == 7:
                replayed = receive(mirror, snapshot)
                trace['replay'] = {
                    'same_id': replayed.receiving_id == accepted.receiving_id,
                    'package_id': replayed.package_id,
                }
            trace['deliveries'].append({
                'delivery': delivery,
                'east': _shape(mirror_interval(mirror, start, end, ['east']))['status'],
                'east_west': _shape(mirror_interval(mirror, start, end,
                                                    ['east', 'west']))['status'],
            })

        trace['closed'] = _shape(mirror_interval(mirror, start, end, ['east', 'west']))
        trace['widened'] = _shape(mirror_interval(mirror, start, receipts[5],
                                                  ['east', 'west']))
    with closing(open_mirror(mirror_path)) as reopened:
        trace['reopened'] = _shape(mirror_interval(reopened, start, end, ['east', 'west']))

    expected_closed = {
        'status': 'ready',
        'events': [
            {'revision': 2, 'correction_id': 'west-missing', 'observation': 'R',
             'source': 'station-E/aug14', 'line': 4, 'position': 1,
             'effects': {'east': ['out', 7, None], 'west': ['in', None, None]}},
            {'revision': 3, 'correction_id': 'back-east', 'observation': 'R',
             'source': 'station-E/aug14', 'line': 4, 'position': 1,
             'effects': {'east': ['in', None, 7], 'west': ['out', None, None]}},
            {'revision': 4, 'correction_id': 'same-reading', 'observation': 'R',
             'source': 'station-E/aug14', 'line': 4, 'position': 1,
             'effects': {'east': ['equal', 7, 7]}},
        ],
        'net': {'east': [0, 0], 'west': [0, 0]},
    }
    checks = {
        'slice_only_pending': trace['slice_only'] == {'status': 'pending'},
        'step3_east_short': trace['deliveries'][2]['east'] == 'pending',
        'step4_east_ready_west_short': (trace['deliveries'][3]['east'] == 'ready'
                                        and trace['deliveries'][3]['east_west'] == 'pending'),
        'step6_both_ready': trace['deliveries'][5]['east_west'] == 'ready',
        'replay_same_receipt': trace['replay']['same_id'],
        'closed_matches_oracle': trace['closed'] == expected_closed,
        'reopened_matches_closed': trace['reopened'] == trace['closed'],
        'widened_ignores_ordinary_post': trace['widened'] == expected_closed,
    }
    scenario = {
        'case_id': 'interval-handoff',
        'input': {'corrections': data['corrections'], 'ordinary_post': data['ordinary_post'],
                  'deliveries': data['deliveries']},
        'trace': trace,
        'expected': {'closed': expected_closed},
        'checks': checks,
        'oracle': ('第 2 版转出东区 7 毫米到西区缺测，第 3 版转回原值，第 4 版等值复测；'
                   '三笔同出 morning/R 第 4 行，跨区只列一次；西区缺测净变 +1 再 −1，'
                   '整个区间各区净变 (0,0)；第 5 版普通入仓占号不计。'),
        'code_paths': ['lake/dispatch/interval.py', 'lake/dispatch/publish.py',
                       'lake/receiving/interval.py', 'lake/query/interval.py'],
    }
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False, sort_keys=True))
    if not all(checks.values()):
        failed = [name for name, ok in checks.items() if not ok]
        raise SystemExit(f'INTERVAL_DEMO_FAILED {failed}')


def main():
    samples = Path(__file__).resolve().parent / 'samples'
    with TemporaryDirectory() as path:
        run_interval_demo(samples, Path(path))


if __name__ == '__main__':
    main()
