import requests
from typing import List, Dict

class TheOddsApiClient:
    BASE_URL = "https://api.the-odds-api.com/v4"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def _request(self, endpoint: str, params: Dict = None) -> Dict:
        if params is None:
            params = {}
        params['apiKey'] = self.api_key
        response = requests.get(f"{self.BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    
    def get_sports(self) -> List[Dict]:
        """Get available sports"""
        return self._request('sports')
    
    def get_odds(self, sport: str = 'soccer_epl', regions: str = 'uk,eu', markets: str = 'h2h') -> List[Dict]:
        """Get odds for a sport (default: English Premier League)"""
        return self._request(f'sports/{sport}/odds', {
            'regions': regions,
            'markets': markets,
            'oddsFormat': 'decimal'
        })
