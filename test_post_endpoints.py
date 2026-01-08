#!/usr/bin/env python3
from sources.alchemy.alchemy import LakeflowConnect
import unittest.mock as mock
import json

# Test the token prices by address POST endpoint
connector = LakeflowConnect({'api_key': 'nbZCASFPBi9sIqyeb8om7', 'network': 'eth-mainnet'})

print('🧪 Testing Alchemy Token Prices by Address (POST) with Mock Data...')
print('=' * 70)
print()

# Mock response for token prices by address
mock_response_data = {
    'data': [
        {
            'network': 'eth-mainnet',
            'address': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
            'prices': [
                {'currency': 'usd', 'value': '1.00'},
                {'currency': 'eur', 'value': '0.92'}
            ]
        },
        {
            'network': 'eth-mainnet',
            'address': '0x6B175474E89094C44Da98b954EessdcD16C26EB',
            'prices': [
                {'currency': 'usd', 'value': '0.93'},
                {'currency': 'eur', 'value': '0.85'}
            ]
        }
    ]
}

def mock_post(url, json=None, **kwargs):
    # Check if this is a token prices by address request
    if 'prices/v1' in url and 'tokens/by-address' in url:
        response = mock.MagicMock()
        response.json.return_value = mock_response_data
        return response
    else:
        raise Exception(f'Unexpected URL in mock: {url}')

# Test with mocked POST call
with mock.patch('requests.post', side_effect=mock_post):
    try:
        table_options = {'addresses': 'eth-mainnet:0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,eth-mainnet:0x6B175474E89094C44Da98b954EessdcD16C26EB'}
        records, offset = connector.read_table('token_prices_by_address', {}, table_options)
        record_list = list(records)

        print(f'✅ Successfully processed {len(record_list)} token price records')
        print()
        print('📈 Retrieved Token Price Data by Address:')
        print('-' * 50)

        for i, record in enumerate(record_list, 1):
            print(f'  {i}. {record["address"][:10]}... ({record["network"]})')
            for price in record['prices']:
                print(f'     {price["currency"].upper()}: ${price["value"]}')
            print()

        # Verify data structure
        assert len(record_list) == 2, f'Expected 2 records, got {len(record_list)}'
        assert all(r['network'] == 'eth-mainnet' for r in record_list), 'All records should be for eth-mainnet'
        assert '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' in [r['address'] for r in record_list], 'USDC address should be present'

        # Check USDC price (should be close to $1)
        usdc_record = next(r for r in record_list if '0xA0b86991' in r['address'])
        usd_price = next(p for p in usdc_record['prices'] if p['currency'] == 'usd')
        assert usd_price['value'] == '1.00', f'USDC USD price should be 1.00, got {usd_price["value"]}'

        print('✅ Data structure validation passed!')
        print('✅ POST endpoint logic works correctly!')
        print('✅ Address-based token pricing is functional!')

    except Exception as e:
        print(f'❌ Error during mock test: {e}')
        import traceback
        traceback.print_exc()

print()
print('🏁 Token Prices by Address (POST) test complete!')