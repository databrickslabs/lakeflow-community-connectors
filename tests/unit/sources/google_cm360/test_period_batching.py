"""Native same-period exports reduce snapshot work without adding reach."""
from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module

OPTIONS = {
    'sync_start_date': '2025-01-01', 'sync_end_date': '2026-10-13',
    'delivery_country_codes': '["US"]', 'reporting_timezone': 'Europe/Paris',
}


def campaign(identifier, end='2026-07-31'):
    return {'campaign_id': identifier, 'advertiser_id': '7',
            'campaign_start_date': '2026-07-01', 'campaign_end_date': end}


def source_with(campaigns, response):
    source = module.GoogleCm360LakeflowConnect({'access_token': 'synthetic', 'profile_id': '1'})
    source._period_country_id = '840'
    source._period_campaigns = Mock(return_value=campaigns)
    source._period_export = Mock(side_effect=response)
    return source


def native(identifier, reach):
    return {'campaign_id': identifier, 'advertiser_id': '7',
            'unique_reach_impression_reach': reach, 'report_file_last_modified_at': '123'}


def read(source):
    rows, offset = source.read_table('reach_report_period', None, OPTIONS)
    assert offset == {}
    return list(rows)


def test_same_period_uses_one_native_report_and_preserves_values():
    def export(report, *_):
        identifiers = [f['id'] for f in report['reachCriteria']['dimensionFilters']
                       if f['dimensionName'] == 'campaign']
        return [native(i, {'1': '3', '2': '-', '3': '0'}[i]) for i in reversed(identifiers)]
    source = source_with([campaign('1'), campaign('2'), campaign('3')], export)
    rows = read(source)
    assert [(r['campaign_id'], r['unique_reach_impression_reach']) for r in rows] == [
        ('1', '3'), ('2', '-'), ('3', '0')]
    assert source._period_export.call_count == 1
    report = source._period_export.call_args.args[0]
    assert report['reachCriteria']['dateRange'] == {'startDate': '2026-07-01', 'endDate': '2026-07-31'}
    assert {'name': 'campaign'} in report['reachCriteria']['dimensions']
    assert not any(d['name'] in {'date', 'month'} for d in report['reachCriteria']['dimensions'])


def test_different_bounds_remain_separate_and_missing_native_row_stays_null():
    source = source_with([campaign('1'), campaign('2', '2026-08-01')], [[], [native('2', '')]])
    rows = read(source)
    assert source._period_export.call_count == 2
    assert [r['period_end'] for r in rows] == ['2026-07-31', '2026-08-01']
    assert [r['unique_reach_impression_reach'] for r in rows] == [None, '']


@pytest.mark.parametrize('values', [
    [native('9', '3')], [native('1', '3'), native('1', '4')],
    [{**native('1', '3'), 'advertiser_id': '8'}],
])
def test_unexpected_or_duplicate_native_rows_abort_snapshot(values):
    source = source_with([campaign('1'), campaign('2')], [values])
    with pytest.raises(module.Cm360DataError, match='unexpected|duplicate'):
        read(source)


def test_failure_after_completed_group_does_not_publish_prefix():
    source = source_with([campaign('1'), campaign('2', '2026-08-01')],
                         [[native('1', '3')], module.Cm360ApiError('HTTP 401')])
    with pytest.raises(module.Cm360ApiError, match='401'):
        read(source)


def test_long_campaign_does_not_enter_native_group():
    source = source_with([campaign('1', '2026-10-01'), campaign('2')], [[native('2', '3')]])
    rows = read(source)
    assert rows[0]['coverage_status'] == 'WINDOW_LIMIT'
    assert rows[0]['unique_reach_impression_reach'] is None
    assert source._period_export.call_count == 1


def test_failure_diagnostics_distinguish_metadata_and_reach_without_token():
    source = source_with([campaign('1')], module.Cm360ApiError('HTTP 401'))
    with pytest.raises(module.Cm360ApiError, match='phase=campaign_reach.*elapsed_seconds=') as caught:
        read(source)
    assert 'synthetic' not in str(caught.value)
    source._period_campaigns.side_effect = module.Cm360ApiError('HTTP 401')
    with pytest.raises(module.Cm360ApiError, match='phase=metadata.*elapsed_seconds='):
        read(source)
