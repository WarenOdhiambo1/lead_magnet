#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from allsports_client import AllSportsApiClient
from theoddsapi_client import TheOddsApiClient
from datetime import datetime, timedelta

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def clean_database():
    """Clear all rows from data tables"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("🧹 Clearing table rows...")
    tables = ['market_odds', 'opportunities', 'predictions', 'league_standings', 
              'matches', 'players', 'coaches', 'teams', 'venues', 'referees', 
              'bookmakers', 'seasons', 'leagues']
    
    for table in tables:
        cur.execute(f"DELETE FROM {table}")
        print(f"  ✓ Cleared {table}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Rows cleared (tables preserved)\n")

def fetch_complete_data():
    """Fetch comprehensive historical and current data"""
    allsports = AllSportsApiClient(os.getenv('ALLSPORTSAPI_KEY'))
    theodds = TheOddsApiClient(os.getenv('THEODDSAPI_KEY'))
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Create seasons first
    seasons_data = [
        ('2022-23', '2022-08-01', '2023-05-31'),
        ('2023-24', '2023-08-01', '2024-05-31'),
        ('2024-25', '2024-08-01', '2025-05-31')
    ]
    
    season_ids = {}
    for season_name, start_date, end_date in seasons_data:
        cur.execute("INSERT INTO seasons (name, start_date, end_date, is_active) VALUES (%s, %s, %s, %s) RETURNING season_id",
                   (season_name, start_date, end_date, season_name == '2024-25'))
        season_ids[season_name] = cur.fetchone()['season_id']
    
    # Create league
    cur.execute("INSERT INTO leagues (name, country, tier, current_season_id) VALUES ('Premier League', 'England', 1, %s) RETURNING league_id",
               (season_ids['2024-25'],))
    league_id = cur.fetchone()['league_id']
    
    teams_map = {}
    venues_map = {}
    referees_map = {}
    
    for season_name, start_date, end_date in seasons_data:
        print(f"\n📅 Fetching {season_name} season...")
        season_id = season_ids[season_name]
        
        # Fetch fixtures
        fixtures = allsports.get_fixtures(152, start_date, end_date)
        print(f"  Found {len(fixtures)} fixtures")
        
        for fixture in fixtures:
            # Venues
            venue_name = fixture.get('event_stadium')
            if venue_name and venue_name not in venues_map:
                cur.execute("INSERT INTO venues (name, city) VALUES (%s, NULL) RETURNING venue_id", (venue_name,))
                venues_map[venue_name] = cur.fetchone()['venue_id']
            
            # Referees
            ref_name = fixture.get('event_referee')
            if ref_name and ref_name not in referees_map:
                cur.execute("INSERT INTO referees (name) VALUES (%s) RETURNING referee_id", (ref_name,))
                referees_map[ref_name] = cur.fetchone()['referee_id']
            
            # Teams
            home_name = fixture.get('event_home_team')
            away_name = fixture.get('event_away_team')
            
            for team_name in [home_name, away_name]:
                if team_name and team_name not in teams_map:
                    venue_id = venues_map.get(venue_name) if venue_name else None
                    cur.execute("INSERT INTO teams (name, league_id, venue_id) VALUES (%s, %s, %s) RETURNING team_id",
                               (team_name, league_id, venue_id))
                    teams_map[team_name] = cur.fetchone()['team_id']
            
            if not home_name or not away_name:
                continue
            
            # Match
            home_id = teams_map[home_name]
            away_id = teams_map[away_name]
            venue_id = venues_map.get(venue_name)
            ref_id = referees_map.get(ref_name)
            
            match_date = fixture.get('event_date')
            match_time = fixture.get('event_time', '15:00')
            kickoff = f"{match_date} {match_time}"
            
            final_result = fixture.get('event_final_result', '')
            home_score = away_score = None
            status = 'SCHEDULED'
            
            if final_result and ' - ' in final_result:
                try:
                    scores = final_result.split(' - ')
                    home_score = int(scores[0])
                    away_score = int(scores[1])
                    status = 'FINISHED'
                except:
                    pass
            
            cur.execute("""
                INSERT INTO matches (league_id, season_id, home_team_id, away_team_id, 
                                   venue_id, referee_id, kickoff_time, home_score, away_score, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (league_id, season_id, home_id, away_id, venue_id, ref_id, kickoff, home_score, away_score, status))
            
            # Players from lineups
            lineups = fixture.get('lineups', {})
            for team_key in ['home_team', 'away_team']:
                if team_key in lineups:
                    team_id = home_id if team_key == 'home_team' else away_id
                    team_name = home_name if team_key == 'home_team' else away_name
                    lineup = lineups[team_key]
                    
                    for player in lineup.get('starting_lineups', []):
                        player_name = player.get('player')
                        if player_name:
                            pos = str(player.get('player_position', 'Unknown'))[:10]
                            cur.execute("""
                                INSERT INTO players (name, position, team_id)
                                VALUES (%s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (player_name, pos, team_id))
        
        conn.commit()
        print(f"  ✓ Processed {season_name}")
    
    # Fetch current standings
    print("\n📊 Fetching current standings...")
    standings = allsports.get_standings(152)
    
    cur.execute("SELECT season_id FROM seasons WHERE name = '2024-25'")
    current_season_id = cur.fetchone()['season_id']
    
    for standing in standings:
        team_name = standing.get('team_name')
        if team_name in teams_map:
            team_id = teams_map[team_name]
            
            cur.execute("""
                INSERT INTO league_standings (
                    league_id, season_id, team_id, position, played, won, drawn, lost,
                    goals_for, goals_against, goal_difference, points
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                league_id, current_season_id, team_id,
                standing.get('standing_place'), standing.get('standing_P'),
                standing.get('standing_W'), standing.get('standing_D'), standing.get('standing_L'),
                standing.get('standing_F'), standing.get('standing_A'),
                standing.get('standing_GD'), standing.get('standing_PTS')
            ))
            
            # Calculate strengths
            played = int(standing.get('standing_P', 0))
            if played > 0:
                gf = int(standing.get('standing_F', 0))
                ga = int(standing.get('standing_A', 0))
                attack = max(0.5, min(2.0, (gf / played) / 1.5))
                defense = max(0.5, min(2.0, (ga / played) / 1.5))
                
                cur.execute("UPDATE teams SET attack_strength = %s, defense_strength = %s WHERE team_id = %s",
                           (attack, defense, team_id))
    
    # Fetch current odds
    print("\n💰 Fetching current odds...")
    odds_data = theodds.get_odds()
    bookmakers_map = {}
    odds_count = 0
    
    for match in odds_data:
        home_team = match.get('home_team')
        away_team = match.get('away_team')
        
        if home_team not in teams_map or away_team not in teams_map:
            continue
        
        cur.execute("""
            SELECT m.match_id FROM matches m
            WHERE m.home_team_id = %s AND m.away_team_id = %s AND m.status = 'SCHEDULED'
            ORDER BY m.kickoff_time LIMIT 1
        """, (teams_map[home_team], teams_map[away_team]))
        
        match_result = cur.fetchone()
        if not match_result:
            continue
        
        match_id = match_result['match_id']
        
        for bookmaker in match.get('bookmakers', []):
            bookie_name = bookmaker.get('title')
            if not bookie_name:
                continue
            
            if bookie_name not in bookmakers_map:
                cur.execute("INSERT INTO bookmakers (name) VALUES (%s) RETURNING bookie_id", (bookie_name,))
                bookmakers_map[bookie_name] = cur.fetchone()['bookie_id']
            
            bookie_id = bookmakers_map[bookie_name]
            
            for market in bookmaker.get('markets', []):
                market_type = market.get('key')
                for outcome in market.get('outcomes', []):
                    cur.execute("""
                        INSERT INTO market_odds (match_id, bookie_id, market_type, selection, odds, timestamp)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """, (match_id, bookie_id, market_type, outcome.get('name'), outcome.get('price')))
                    odds_count += 1
    
    conn.commit()
    
    print(f"\n✅ Data Fetch Complete:")
    print(f"   Leagues: 1")
    print(f"   Seasons: {len(seasons_data)}")
    print(f"   Teams: {len(teams_map)}")
    print(f"   Venues: {len(venues_map)}")
    print(f"   Referees: {len(referees_map)}")
    print(f"   Bookmakers: {len(bookmakers_map)}")
    print(f"   Market Odds: {odds_count}")
    
    cur.execute("SELECT COUNT(*) as cnt FROM matches")
    print(f"   Matches: {cur.fetchone()['cnt']}")
    
    cur.execute("SELECT COUNT(*) as cnt FROM players")
    print(f"   Players: {cur.fetchone()['cnt']}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("DATABASE REBUILD - COMPREHENSIVE DATA FETCH")
    print("=" * 80)
    
    clean_database()
    fetch_complete_data()
    
    print("\n" + "=" * 80)
    print("✅ REBUILD COMPLETE - Run generate_predictions.py next")
    print("=" * 80)
