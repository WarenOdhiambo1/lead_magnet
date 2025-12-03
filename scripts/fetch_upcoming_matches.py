#!/usr/bin/env python3
"""
Fetch upcoming Premier League matches from AllSportsApi
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

API_KEY = os.getenv('ALLSPORTSAPI_KEY')
BASE_URL = "https://apiv2.allsportsapi.com/football/"

def fetch_upcoming_fixtures():
    """Fetch upcoming fixtures from AllSportsApi"""
    print("\n🔄 Fetching upcoming Premier League fixtures...\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get current season
    cur.execute("SELECT season_id FROM seasons ORDER BY season_id DESC LIMIT 1")
    season = cur.fetchone()
    season_id = season['season_id'] if season else 1
    
    # Fetch fixtures in 14-day chunks (API limit is 15 days)
    today = datetime.now()
    all_fixtures = []
    
    try:
        # Fetch 3 chunks: next 14 days, next 14-28 days, next 28-42 days
        for chunk in range(3):
            start_date = (today + timedelta(days=chunk * 14)).strftime('%Y-%m-%d')
            end_date = (today + timedelta(days=(chunk + 1) * 14 - 1)).strftime('%Y-%m-%d')
            
            url = f"{BASE_URL}?met=Fixtures&APIkey={API_KEY}&league_id=152&from={start_date}&to={end_date}"
            
            print(f"🔍 Chunk {chunk + 1}: {start_date} to {end_date}")
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                print(f"   ⚠️  HTTP {response.status_code}")
                continue
            
            data = response.json()
            
            if data.get('success') == 1 and data.get('result'):
                fixtures = data.get('result', [])
                all_fixtures.extend(fixtures)
                print(f"   ✅ Found {len(fixtures)} fixtures")
            else:
                print(f"   ⚠️  No fixtures or error")
        
        print(f"\n📊 Total fixtures found: {len(all_fixtures)}\n")
        
        if len(all_fixtures) == 0:
            print("⚠️  No upcoming fixtures found. This is normal during off-season.")
            print("💡 The trained model is ready. Use it when new fixtures are available.\n")
            return
        
        fixtures = all_fixtures
        
        added = 0
        updated = 0
        
        for fixture in fixtures:
            # Get or create teams
            home_team_id = get_or_create_team(cur, fixture['event_home_team'], fixture['home_team_key'])
            away_team_id = get_or_create_team(cur, fixture['event_away_team'], fixture['away_team_key'])
            
            # Get or create venue
            venue_id = get_or_create_venue(cur, fixture.get('event_stadium'), fixture.get('event_stadium'))
            
            # Get or create referee
            referee_id = None
            if fixture.get('event_referee'):
                referee_id = get_or_create_referee(cur, fixture['event_referee'])
            
            # Parse kickoff time
            kickoff_str = f"{fixture['event_date']} {fixture['event_time']}"
            try:
                kickoff_time = datetime.strptime(kickoff_str, '%Y-%m-%d %H:%M')
            except:
                kickoff_time = datetime.strptime(fixture['event_date'], '%Y-%m-%d')
            
            # Determine status
            status = 'SCHEDULED'
            if fixture['event_status'] in ['Finished', 'FINISHED', 'FT']:
                status = 'FINISHED'
            elif fixture['event_status'] in ['Live', 'LIVE', 'HT']:
                status = 'LIVE'
            
            # Check if match already exists (by teams and approximate time)
            cur.execute("""
                SELECT match_id, status FROM matches
                WHERE home_team_id = %s 
                  AND away_team_id = %s
                  AND kickoff_time::date = %s::date
            """, (home_team_id, away_team_id, kickoff_time))
            
            existing = cur.fetchone()
            
            if existing:
                # Update existing match
                cur.execute("""
                    UPDATE matches
                    SET kickoff_time = %s,
                        status = %s,
                        venue_id = %s,
                        referee_id = %s
                    WHERE match_id = %s
                    RETURNING match_id
                """, (kickoff_time, status, venue_id, referee_id, existing['match_id']))
                updated += 1
            else:
                # Insert new match
                cur.execute("""
                    INSERT INTO matches (
                        league_id, season_id, home_team_id, away_team_id, venue_id, referee_id,
                        kickoff_time, status
                    )
                    VALUES (2, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id
                """, (season_id, home_team_id, away_team_id, venue_id, referee_id, 
                      kickoff_time, status))
                
                if status != 'FINISHED':
                    added += 1
                    print(f"✅ {fixture['event_home_team']} vs {fixture['event_away_team']} - {kickoff_time.strftime('%Y-%m-%d %H:%M')} [{status}]")
                else:
                    added += 1
        
        conn.commit()
        print(f"\n✅ Added {added} upcoming matches, updated {updated} existing matches")
        
    except requests.exceptions.Timeout:
        print(f"❌ Error: API request timed out. Try again later.")
        conn.rollback()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_or_create_team(cur, team_name, api_team_id):
    """Get or create team by name"""
    # Try to find by name (case-insensitive)
    cur.execute("""
        SELECT team_id FROM teams 
        WHERE LOWER(name) = LOWER(%s) 
        LIMIT 1
    """, (team_name,))
    team = cur.fetchone()
    if team:
        return team['team_id']
    
    # Create new team
    cur.execute("""
        INSERT INTO teams (name, league_id, attack_strength, defense_strength, elo_rating)
        VALUES (%s, 2, 1.0, 1.0, 1500)
        ON CONFLICT (name, league_id) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING team_id
    """, (team_name,))
    return cur.fetchone()['team_id']

def get_or_create_venue(cur, venue_name, api_venue_id):
    """Get or create venue"""
    if not venue_name:
        return None
    
    cur.execute("SELECT venue_id FROM venues WHERE LOWER(name) = LOWER(%s)", (venue_name,))
    venue = cur.fetchone()
    if venue:
        return venue['venue_id']
    
    cur.execute("""
        INSERT INTO venues (name, city, capacity)
        VALUES (%s, 'Unknown', 0)
        RETURNING venue_id
    """, (venue_name,))
    return cur.fetchone()['venue_id']

def get_or_create_referee(cur, referee_name):
    """Get or create referee"""
    if not referee_name:
        return None
    
    cur.execute("SELECT referee_id FROM referees WHERE LOWER(name) = LOWER(%s)", (referee_name,))
    referee = cur.fetchone()
    if referee:
        return referee['referee_id']
    
    cur.execute("""
        INSERT INTO referees (name)
        VALUES (%s)
        RETURNING referee_id
    """, (referee_name,))
    return cur.fetchone()['referee_id']

if __name__ == '__main__':
    fetch_upcoming_fixtures()
    print("\n💡 Now run: python ml_ensemble_ultimate.py --predict-only")
