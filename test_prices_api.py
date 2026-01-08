#!/usr/bin/env python3
from sources.alchemy.alchemy import LakeflowConnect
import unittest.mock as mock
import json

# Test the token prices API with mocked responses
connector = LakeflowConnect({'api_key': 'nbZCASFPBi9sIqyeb8om7', 'network': 'eth-mainnet'})

print('🧪 Testing Alchemy Token Prices API with Mock Data...')
print()

# Mock response for token prices
mock_response_data = {
    'data': [
        {
            'symbol': 'ETH',
            'prices': [
                {'currency': 'usd', 'value': '3245.67'},
                {'currency': 'eur', 'value': '2987.23'}
            ]
        },
        {
            'symbol': 'BTC',
            'prices': [
                {'currency': 'usd', 'value': '65432.10'},
                {'currency': 'eur', 'value': '60123.45'}
            ]
        }
    ]
}

def mock_get(url, params=None, **kwargs):
    # Check if this is a token prices request
    if 'prices/v1' in url and 'tokens/by-symbol' in url:
        response = mock.MagicMock()
        response.json.return_value = mock_response_data
        return response
    else:
        raise Exception(f'Unexpected URL in mock: {url}')

# Test with mocked API call
with mock.patch('requests.get', side_effect=mock_get):
    try:
        table_options = {'symbols': 'ETH,BTC'}
        records, offset = connector.read_table('token_prices', {}, table_options)
        record_list = list(records)

        print(f'✅ Successfully processed {len(record_list)} records')
        print()
        print('📈 Retrieved Data:')
        for i, record in enumerate(record_list, 1):
            print(f'  Record {i}:')
            print(f'    Symbol: {record["symbol"]}')
            print(f'    Prices: {len(record["prices"])} currencies')
            for price in record['prices']:
                print(f'      - {price["currency"].upper()}: ${price["value"]}')
            print()

        # Verify data structure
        assert len(record_list) == 2, f'Expected 2 records, got {len(record_list)}'
        assert record_list[0]['symbol'] == 'ETH', f'First record should be ETH, got {record_list[0]["symbol"]}'
        assert len(record_list[0]['prices']) == 2, f'ETH should have 2 price entries, got {len(record_list[0]["prices"])}'

        print('✅ Data structure validation passed!')
        print('✅ Token Prices API logic works correctly!')

    except Exception as e:
        print(f'❌ Error during mock test: {e}')
        import traceback
        traceback.print_exc()

print()
print('🏁 Mock Token Prices API test complete!')