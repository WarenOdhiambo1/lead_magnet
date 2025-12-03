#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from theoddsapi_client import TheOddsApiClient

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def fetch_odds():
    client = TheOddsApiClient('987e61b1ed5b257c09f256c95a55b966')
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("Fetching EPL odds from The Odds API...")
    odds_data = client.get_odds()
    print(f"Found {len(odds_data)} matches with odds\n")
    
    bookmakers_added = set()
    odds_count = 0
    matches_processed = 0
    
    for match in odds_data:
        home_team = match.get('home_team')
        away_team = match.get('away_team')
        
        # Find match in database
        cur.execute("""
            SELECT m.match_id FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE ht.name = %s AND at.name = %s
            LIMIT 1
        """, (home_team, away_team))
        match_result = cur.fetchone()
        
        if not match_result:
            print(f"Match not found: {home_team} vs {away_team}")
            continue
        
        internal_match_id = match_result['match_id']
        matches_processed += 1
        
        # Process bookmakers
        for bookmaker in match.get('bookmakers', []):
            bookmaker_name = bookmaker.get('title')
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
            for market in bookmaker.get('markets', []):
                market_type = market.get('key')
                for outcome in market.get('outcomes', []):
                    cur.execute("""
                        INSERT INTO market_odds (match_id, bookie_id, market_type, selection, odds, timestamp)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """, (internal_match_id, bookmaker_id, market_type, outcome.get('name'), outcome.get('price')))
                    odds_count += 1
        
        print(f"✓ {home_team} vs {away_team}")
    
    conn.commit()
    print(f"\n✅ Odds Fetch Complete:")
    print(f"   Matches: {matches_processed}")
    print(f"   Bookmakers: {len(bookmakers_added)}")
    print(f"   Market Odds: {odds_count}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    try:
        fetch_odds()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
