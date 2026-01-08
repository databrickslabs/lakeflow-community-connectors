#!/usr/bin/env python3
"""
Script to help validate Particle access tokens.

Usage:
1. Run: python get_particle_token.py YOUR_TOKEN_HERE
2. Or just run: python get_particle_token.py
   (it will prompt for token)
"""

import sys
import requests
import getpass

def test_token(token):
    """Test if a Particle access token is valid."""
    url = 'https://api.particle.io/v1/user'
    headers = {'Authorization': f'Bearer {token}'}
    
    print(f"Testing token: {token[:10]}...")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: Token is valid!")
            user_data = response.json()
            print(f"   Account: {user_data.get('username', 'Unknown')}")
            return True
        else:
            print(f"❌ FAILED: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1:
        token = sys.argv[1]
    else:
        token = getpass.getpass("Enter your Particle access token: ")
    
    test_token(token)
