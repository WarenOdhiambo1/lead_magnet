#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from allsports_client import AllSportsApiClient
from datetime import datetime, timedelta

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def fetch_historical():
    client = AllSportsApiClient(os.getenv('ALLSPORTSAPI_KEY'))
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Fetch 2024-25 season data (Aug 2024 - May 2025)
    print("Fetching 2024-25 Premier League season data...")
    
    # Get league and season
    cur.execute("SELECT league_id FROM leagues WHERE name = 'Premier League' LIMIT 1")
    league = cur.fetchone()
    if not league:
        cur.execute("INSERT INTO leagues (name, country, tier) VALUES ('Premier League', 'England', 1) RETURNING league_id")
        league_id = cur.fetchone()['league_id']
    else:
        league_id = league['league_id']
    
    cur.execute("SELECT season_id FROM seasons WHERE name = '2024-25' AND league_id = %s LIMIT 1", (league_id,))
    season = cur.fetchone()
    if not season:
        cur.execute("INSERT INTO seasons (league_id, name, start_date, end_date) VALUES (%s, '2024-25', '2024-08-01', '2025-05-31') RETURNING season_id", (league_id,))
        season_id = cur.fetchone()['season_id']
    else:
        season_id = season['season_id']
    
    # Fetch fixtures from Aug 2024 to now
    fixtures = client.get_fixtures(152, '2024-08-01', datetime.now().strftime('%Y-%m-%d'))
    print(f"Found {len(fixtures)} fixtures\n")
    
    teams_added = {}
    matches_added = 0
    
    for fixture in fixtures:
        home_name = fixture.get('event_home_team')
        away_name = fixture.get('event_away_team')
        
        if not home_name or not away_name:
            continue
        
        # Add teams
        for team_name in [home_name, away_name]:
            if team_name not in teams_added:
                cur.execute("SELECT team_id FROM teams WHERE name = %s", (team_name,))
                team = cur.fetchone()
                if not team:
                    cur.execute("INSERT INTO teams (name, league_id) VALUES (%s, %s) RETURNING team_id", (team_name, league_id))
                    teams_added[team_name] = cur.fetchone()['team_id']
                else:
                    teams_added[team_name] = team['team_id']
        
        home_id = teams_added[home_name]
        away_id = teams_added[away_name]
        
        # Parse match date
        match_date = fixture.get('event_date')
        match_time = fixture.get('event_time', '15:00')
        kickoff = f"{match_date} {match_time}"
        
        # Get scores
        final_result = fixture.get('event_final_result', '')
        home_score = None
        away_score = None
        status = 'SCHEDULED'
        
        if final_result and ' - ' in final_result:
            try:
                scores = final_result.split(' - ')
                home_score = int(scores[0])
                away_score = int(scores[1])
                status = 'FINISHED'
            except:
                pass
        
        # Insert match
        cur.execute("""
            INSERT INTO matches (league_id, season_id, home_team_id, away_team_id, kickoff_time, home_score, away_score, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (league_id, season_id, home_id, away_id, kickoff, home_score, away_score, status))
        matches_added += 1
        
        if matches_added % 50 == 0:
            print(f"Processed {matches_added} matches...")
            conn.commit()
    
    # Fetch current standings
    print("\nFetching standings...")
    standings = client.get_standings(152)
    
    for standing in standings:
        team_name = standing.get('team_name')
        if team_name in teams_added:
            team_id = teams_added[team_name]
            
            cur.execute("""
                INSERT INTO league_standings (
                    league_id, season_id, team_id, position, played, won, drawn, lost,
                    goals_for, goals_against, goal_difference, points
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (league_id, season_id, team_id) 
                DO UPDATE SET position = EXCLUDED.position, played = EXCLUDED.played,
                won = EXCLUDED.won, drawn = EXCLUDED.drawn, lost = EXCLUDED.lost,
                goals_for = EXCLUDED.goals_for, goals_against = EXCLUDED.goals_against,
                goal_difference = EXCLUDED.goal_difference, points = EXCLUDED.points
            """, (
                league_id, season_id, team_id,
                standing.get('standing_place'),
                standing.get('standing_P'),
                standing.get('standing_W'),
                standing.get('standing_D'),
                standing.get('standing_L'),
                standing.get('standing_F'),
                standing.get('standing_A'),
                standing.get('standing_GD'),
                standing.get('standing_PTS')
            ))
            
            # Calculate team strengths
            played = int(standing.get('standing_P', 0))
            if played > 0:
                gf = int(standing.get('standing_F', 0))
                ga = int(standing.get('standing_A', 0))
                attack = (gf / played) / 1.5
                defense = (ga / played) / 1.5
                
                cur.execute("""
                    UPDATE teams 
                    SET attack_strength = %s, defense_strength = %s
                    WHERE team_id = %s
                """, (attack, defense, team_id))
    
    conn.commit()
    
    print(f"\n✅ Historical Data Fetch Complete:")
    print(f"   Teams: {len(teams_added)}")
    print(f"   Matches: {matches_added}")
    print(f"   Standings: {len(standings)}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    fetch_historical()
