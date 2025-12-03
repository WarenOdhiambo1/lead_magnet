#!/usr/bin/env python3
from theoddsapi_client import TheOddsApiClient
import json

client = TheOddsApiClient('987e61b1ed5b257c09f256c95a55b966')

print("=" * 80)
print("AVAILABLE SPORTS")
print("=" * 80)
sports = client.get_sports()
for sport in sports:
    print(f"- {sport.get('key')}: {sport.get('title')} ({sport.get('group')})")

print("\n" + "=" * 80)
print("EPL ODDS SAMPLE")
print("=" * 80)
odds = client.get_odds()
print(f"\nTotal matches: {len(odds)}")

if odds:
    match = odds[0]
    print(f"\nSample Match:")
    print(json.dumps(match, indent=2))
