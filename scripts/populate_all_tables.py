#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from allsports_client import AllSportsApiClient

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def populate_all():
    client = AllSportsApiClient(os.getenv('ALLSPORTSAPI_KEY'))
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get Premier League fixtures with full details
    fixtures = client.get_fixtures(152, '2024-12-01', '2026-01-31')
    
    print(f"Processing {len(fixtures)} fixtures...")
    
    venues_added = set()
    referees_added = set()
    players_added = set()
    coaches_added = set()
    
    for fixture in fixtures:
        # Populate venues
        venue_name = fixture.get('event_stadium')
        if venue_name and venue_name not in venues_added:
            cur.execute("""
                INSERT INTO venues (name, city, capacity)
                VALUES (%s, NULL, NULL)
                ON CONFLICT DO NOTHING
            """, (venue_name,))
            venues_added.add(venue_name)
        
        # Populate referees
        referee_name = fixture.get('event_referee')
        if referee_name and referee_name not in referees_added:
            cur.execute("""
                INSERT INTO referees (name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (referee_name,))
            referees_added.add(referee_name)
        
        # Populate lineups (players & coaches)
        lineups = fixture.get('lineups', {})
        
        # Home team
        home_team_name = fixture.get('event_home_team')
        home_team_id = None
        if home_team_name:
            cur.execute("SELECT team_id FROM teams WHERE name = %s", (home_team_name,))
            team_result = cur.fetchone()
            if team_result:
                home_team_id = team_result['team_id']
        
        if home_team_id and 'home_team' in lineups:
            home_lineup = lineups['home_team']
            
            # Coach
            coach = home_lineup.get('coach', [{}])[0] if home_lineup.get('coach') else {}
            coach_name = coach.get('coach')
            if coach_name and coach_name not in coaches_added:
                cur.execute("""
                    INSERT INTO coaches (name, current_team_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (coach_name, home_team_id))
                coaches_added.add(coach_name)
            
            # Players
            for player in home_lineup.get('starting_lineups', []):
                player_name = player.get('player')
                if player_name and player_name not in players_added:
                    pos = str(player.get('player_position', 'Unknown'))[:10]
                    cur.execute("""
                        INSERT INTO players (name, position, team_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (player_name, pos, home_team_id))
                    players_added.add(player_name)
        
        # Away team
        away_team_name = fixture.get('event_away_team')
        away_team_id = None
        if away_team_name:
            cur.execute("SELECT team_id FROM teams WHERE name = %s", (away_team_name,))
            team_result = cur.fetchone()
            if team_result:
                away_team_id = team_result['team_id']
        
        if away_team_id and 'away_team' in lineups:
            away_lineup = lineups['away_team']
            
            # Coach
            coach = away_lineup.get('coach', [{}])[0] if away_lineup.get('coach') else {}
            coach_name = coach.get('coach')
            if coach_name and coach_name not in coaches_added:
                cur.execute("""
                    INSERT INTO coaches (name, current_team_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (coach_name, away_team_id))
                coaches_added.add(coach_name)
            
            # Players
            for player in away_lineup.get('starting_lineups', []):
                player_name = player.get('player')
                if player_name and player_name not in players_added:
                    pos = str(player.get('player_position', 'Unknown'))[:10]
                    cur.execute("""
                        INSERT INTO players (name, position, team_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (player_name, pos, away_team_id))
                    players_added.add(player_name)
    
    # Populate bookmakers and odds from fixtures that have odds data
    bookmakers_added = set()
    odds_count = 0
    
    for fixture in fixtures[:20]:  # Process first 20 fixtures with odds
        match_id = fixture.get('event_key')
        if not match_id:
            continue
        
        try:
            odds_data = client.get_odds(match_id)
            if not odds_data:
                continue
                
            # Get our internal match_id
            home_name = fixture.get('event_home_team')
            away_name = fixture.get('event_away_team')
            cur.execute("""
                SELECT m.match_id FROM matches m
                JOIN teams ht ON m.home_team_id = ht.team_id
                JOIN teams at ON m.away_team_id = at.team_id
                WHERE ht.name = %s AND at.name = %s
                LIMIT 1
            """, (home_name, away_name))
            match_result = cur.fetchone()
            if not match_result:
                continue
            internal_match_id = match_result['match_id']
            
            for odd in odds_data:
                bookmaker_name = odd.get('bookmaker_name')
                if not bookmaker_name:
                    continue
                    
                if bookmaker_name not in bookmakers_added:
                    cur.execute("""
                        INSERT INTO bookmakers (name)
                        VALUES (%s)
                        ON CONFLICT (name) DO NOTHING
                        RETURNING bookie_id
                    """, (bookmaker_name,))
                    result = cur.fetchone()
                    if result:
                        bookmaker_id = result['bookie_id']
                    else:
                        cur.execute("SELECT bookie_id FROM bookmakers WHERE name = %s", (bookmaker_name,))
                        bookmaker_id = cur.fetchone()['bookie_id']
                    bookmakers_added.add(bookmaker_name)
                else:
                    cur.execute("SELECT bookie_id FROM bookmakers WHERE name = %s", (bookmaker_name,))
                    bookmaker_id = cur.fetchone()['bookie_id']
                
                # Insert odds
                for market in odd.get('odds', []):
                    market_type = market.get('type')
                    for value in market.get('values', []):
                        cur.execute("""
                            INSERT INTO market_odds (match_id, bookie_id, market_type, outcome, odds, timestamp)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT DO NOTHING
                        """, (internal_match_id, bookmaker_id, market_type, value.get('value'), value.get('odd')))
                        odds_count += 1
        except Exception as e:
            print(f"Error fetching odds for fixture {match_id}: {e}")
    
    conn.commit()
    
    print(f"\n✅ Population Complete:")
    print(f"   Venues: {len(venues_added)}")
    print(f"   Referees: {len(referees_added)}")
    print(f"   Players: {len(players_added)}")
    print(f"   Coaches: {len(coaches_added)}")
    print(f"   Bookmakers: {len(bookmakers_added)}")
    print(f"   Market Odds: {odds_count}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    try:
        populate_all()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
