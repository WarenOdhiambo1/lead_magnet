from allsports_client import AllSportsApiClient
import os
from dotenv import load_dotenv
import json

load_dotenv()
client = AllSportsApiClient(os.getenv('ALLSPORTSAPI_KEY'))
fixtures = client.get_fixtures(152, '2024-12-01', '2024-12-31')
print(f'Total fixtures: {len(fixtures)}')
if fixtures:
    f = fixtures[0]
    print(f"\nSample fixture keys: {list(f.keys())}")
    print(f"\nVenue: {f.get('match_stadium')}")
    print(f"City: {f.get('match_city')}")
    print(f"Referee: {f.get('match_referee')}")
    print(f"Home team: {f.get('match_hometeam_name')}")
    print(f"Away team: {f.get('match_awayteam_name')}")
    print(f"Has lineups: {'lineups' in f}")
    if 'lineups' in f:
        print(f"Lineups keys: {list(f['lineups'].keys())}")
