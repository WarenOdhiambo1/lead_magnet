import requests
import os
from typing import List, Dict, Any
from datetime import datetime

class AllSportsApiClient:
    BASE_URL = "https://apiv2.allsportsapi.com/football/"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def _request(self, params: Dict[str, Any]) -> Dict:
        params['APIkey'] = self.api_key
        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_leagues(self) -> List[Dict]:
        """Get all available leagues"""
        return self._request({'met': 'Leagues'}).get('result', [])
    
    def get_fixtures(self, league_id: int, from_date: str, to_date: str) -> List[Dict]:
        """Get fixtures for a league between dates (YYYY-MM-DD)"""
        return self._request({
            'met': 'Fixtures',
            'leagueId': league_id,
            'from': from_date,
            'to': to_date
        }).get('result', [])
    
    def get_standings(self, league_id: int) -> List[Dict]:
        """Get league standings"""
        result = self._request({
            'met': 'Standings',
            'leagueId': league_id
        }).get('result', {})
        return result.get('total', []) if isinstance(result, dict) else []
    
    def get_teams(self, league_id: int) -> List[Dict]:
        """Get teams in a league"""
        return self._request({
            'met': 'Teams',
            'leagueId': league_id
        }).get('result', [])
    
    def get_h2h(self, first_team_id: int, second_team_id: int) -> List[Dict]:
        """Get head-to-head matches"""
        return self._request({
            'met': 'H2H',
            'firstTeamId': first_team_id,
            'secondTeamId': second_team_id
        }).get('result', [])
    
    def get_odds(self, match_id: int) -> List[Dict]:
        """Get odds for a match"""
        return self._request({
            'met': 'Odds',
            'matchId': match_id
        }).get('result', [])
