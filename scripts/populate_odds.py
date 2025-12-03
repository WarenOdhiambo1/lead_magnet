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

def populate_odds():
    client = AllSportsApiClient(os.getenv('ALLSPORTSAPI_KEY'))
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    fixtures = client.get_fixtures(152, '2024-12-01', '2025-01-31')
    print(f"Fetching odds for {len(fixtures)} fixtures...")
    
    bookmakers_added = set()
    odds_count = 0
    processed = 0
    
    for fixture in fixtures:
        event_key = fixture.get('event_key')
        home_name = fixture.get('event_home_team')
        away_name = fixture.get('event_away_team')
        
        if not all([event_key, home_name, away_name]):
            continue
        
        # Get internal match_id
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
        
        try:
            odds_data = client.get_odds(event_key)
            if not odds_data or isinstance(odds_data, str):
                continue
            
            for odd in odds_data:
                if not isinstance(odd, dict):
                    continue
                bookmaker_name = odd.get('bookmaker_name')
                if not bookmaker_name:
                    continue
                
                # Insert bookmaker
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
            
            processed += 1
            if processed % 10 == 0:
                print(f"Processed {processed} matches, {len(bookmakers_added)} bookmakers, {odds_count} odds")
                conn.commit()
                
        except Exception as e:
            print(f"Error for {home_name} vs {away_name}: {e}")
    
    conn.commit()
    print(f"\n✅ Odds Population Complete:")
    print(f"   Bookmakers: {len(bookmakers_added)}")
    print(f"   Market Odds: {odds_count}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    try:
        populate_odds()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
