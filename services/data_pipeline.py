import psycopg2
from psycopg2.extras import RealDictCursor
from allsports_client import AllSportsApiClient
from datetime import datetime, timedelta
import os

class DataPipeline:
    def __init__(self, api_client: AllSportsApiClient):
        self.client = api_client
        
    def get_db(self):
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "student_finance_dream"),
            user=os.getenv("DB_USER", "Waren_Dev"),
            password=os.getenv("DB_PASSWORD", ""),
            cursor_factory=RealDictCursor
        )
    
    def ingest_league(self, league_id: int, league_name: str, country: str):
        """Insert or update league"""
        with self.get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO leagues (league_id, name, country)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (league_id) DO UPDATE 
                    SET name = EXCLUDED.name, country = EXCLUDED.country
                    RETURNING league_id
                """, (league_id, league_name, country))
                conn.commit()
                return cur.fetchone()['league_id']
    
    def ingest_season(self, season_name: str, start_date: str, end_date: str):
        """Insert or update season"""
        with self.get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO seasons (name, start_date, end_date, is_active)
                    VALUES (%s, %s, %s, true)
                    ON CONFLICT DO NOTHING
                    RETURNING season_id
                """, (season_name, start_date, end_date))
                result = cur.fetchone()
                conn.commit()
                return result['season_id'] if result else None
    
    def ingest_teams(self, league_id: int):
        """Fetch and insert teams for a league"""
        teams = self.client.get_teams(league_id)
        
        with self.get_db() as conn:
            with conn.cursor() as cur:
                for team in teams:
                    cur.execute("""
                        INSERT INTO teams (team_id, name, league_id, elo_rating)
                        VALUES (%s, %s, %s, 1500.00)
                        ON CONFLICT (team_id) DO UPDATE 
                        SET name = EXCLUDED.name, league_id = EXCLUDED.league_id
                    """, (
                        int(team['team_key']),
                        team['team_name'],
                        league_id
                    ))
                conn.commit()
        
        return len(teams)
    
    def ingest_standings(self, league_id: int, season_id: int):
        """Fetch and insert league standings"""
        standings = self.client.get_standings(league_id)
        
        with self.get_db() as conn:
            with conn.cursor() as cur:
                for standing in standings:
                    team_id = int(standing.get('team_key', 0))
                    team_name = standing.get('standing_team', 'Unknown')
                    
                    # Ensure team exists first
                    cur.execute("""
                        INSERT INTO teams (team_id, name, league_id, elo_rating)
                        VALUES (%s, %s, %s, 1500.00)
                        ON CONFLICT (team_id) DO UPDATE SET name = EXCLUDED.name
                    """, (team_id, team_name, league_id))
                    
                    # Insert standing
                    cur.execute("""
                        INSERT INTO league_standings 
                        (league_id, season_id, team_id, position, points, goal_difference)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        league_id,
                        season_id,
                        team_id,
                        int(standing.get('standing_place', 0)),
                        int(standing.get('standing_PTS', 0)),
                        int(standing.get('standing_GD', 0))
                    ))
                conn.commit()
        
        return len(standings)
    
    def ingest_fixtures(self, league_id: int, season_id: int, days_ahead: int = 30):
        """Fetch and insert upcoming fixtures"""
        today = datetime.now().strftime('%Y-%m-%d')
        future = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        
        fixtures = self.client.get_fixtures(league_id, today, future)
        
        with self.get_db() as conn:
            with conn.cursor() as cur:
                for fixture in fixtures:
                    match_datetime = f"{fixture['event_date']} {fixture['event_time']}"
                    
                    # Parse scores from event_final_result (e.g., "2 - 1")
                    home_score = None
                    away_score = None
                    if fixture.get('event_final_result'):
                        try:
                            scores = fixture['event_final_result'].split(' - ')
                            home_score = int(scores[0])
                            away_score = int(scores[1])
                        except:
                            pass
                    
                    cur.execute("""
                        INSERT INTO matches 
                        (match_id, league_id, season_id, home_team_id, away_team_id, 
                         kickoff_time, home_score, away_score, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_id) DO UPDATE 
                        SET home_score = EXCLUDED.home_score,
                            away_score = EXCLUDED.away_score,
                            status = EXCLUDED.status
                        RETURNING match_id
                    """, (
                        int(fixture['event_key']),
                        league_id,
                        season_id,
                        int(fixture['home_team_key']),
                        int(fixture['away_team_key']),
                        match_datetime,
                        home_score,
                        away_score,
                        fixture['event_status']
                    ))
                conn.commit()
        
        return len(fixtures)
    
    def ingest_odds(self, match_id: int):
        """Fetch and insert odds for a match"""
        odds_data = self.client.get_odds(match_id)
        
        if not odds_data:
            return 0
        
        with self.get_db() as conn:
            with conn.cursor() as cur:
                count = 0
                for odd in odds_data:
                    # Get or create bookmaker
                    cur.execute("""
                        INSERT INTO bookmakers (name)
                        VALUES (%s)
                        ON CONFLICT DO NOTHING
                        RETURNING bookie_id
                    """, (odd.get('bookmaker_name', 'Unknown'),))
                    
                    result = cur.fetchone()
                    if result:
                        bookie_id = result['bookie_id']
                    else:
                        cur.execute("SELECT bookie_id FROM bookmakers WHERE name = %s", 
                                  (odd.get('bookmaker_name', 'Unknown'),))
                        bookie_id = cur.fetchone()['bookie_id']
                    
                    # Insert odds for each market
                    for market_type, value in odd.items():
                        if market_type.startswith('odd_') and value:
                            selection = market_type.replace('odd_', '').upper()
                            
                            cur.execute("""
                                INSERT INTO market_odds 
                                (match_id, bookie_id, market_type, selection, odds)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (match_id, bookie_id, '1X2', selection, float(value)))
                            count += 1
                
                conn.commit()
        
        return count
    
    def calculate_team_strengths(self, league_id: int):
        """Calculate attack/defense strengths from standings"""
        with self.get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT team_id, 
                           COALESCE(points::float / NULLIF(position, 0), 1.0) as strength_factor
                    FROM league_standings
                    WHERE league_id = %s
                """, (league_id,))
                
                standings = cur.fetchall()
                
                for standing in standings:
                    attack_strength = 0.8 + (standing['strength_factor'] * 0.4)
                    defense_strength = 1.2 - (standing['strength_factor'] * 0.4)
                    
                    cur.execute("""
                        UPDATE teams 
                        SET attack_strength = %s, defense_strength = %s
                        WHERE team_id = %s
                    """, (attack_strength, defense_strength, standing['team_id']))
                
                conn.commit()
    
    def run_full_ingestion(self, league_id: int, league_name: str, country: str):
        """Run complete data ingestion pipeline"""
        print(f"🚀 Starting data ingestion for {league_name}...")
        
        # 1. Ingest league
        print("📊 Ingesting league...")
        self.ingest_league(league_id, league_name, country)
        
        # 2. Create season
        print("📅 Creating season...")
        season_name = f"{datetime.now().year}/{datetime.now().year + 1}"
        season_id = self.ingest_season(
            season_name,
            f"{datetime.now().year}-08-01",
            f"{datetime.now().year + 1}-05-31"
        )
        
        # 3. Ingest teams
        print("⚽ Ingesting teams...")
        teams_count = self.ingest_teams(league_id)
        print(f"   ✅ {teams_count} teams ingested")
        
        # 4. Ingest standings
        print("📈 Ingesting standings...")
        standings_count = self.ingest_standings(league_id, season_id)
        print(f"   ✅ {standings_count} standings ingested")
        
        # 5. Calculate team strengths
        print("💪 Calculating team strengths...")
        self.calculate_team_strengths(league_id)
        
        # 6. Ingest fixtures
        print("🗓️  Ingesting fixtures...")
        fixtures_count = self.ingest_fixtures(league_id, season_id)
        print(f"   ✅ {fixtures_count} fixtures ingested")
        
        print(f"\n✅ Data ingestion complete for {league_name}!")
        
        return {
            'league_id': league_id,
            'teams': teams_count,
            'standings': standings_count,
            'fixtures': fixtures_count
        }
