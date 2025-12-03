#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor(cursor_factory=RealDictCursor)

# Calculate from match history
cur.execute("""
    SELECT 
        t.team_id, 
        t.name,
        COUNT(m.match_id) as played,
        SUM(CASE WHEN m.home_team_id = t.team_id THEN m.home_score ELSE m.away_score END) as goals_for,
        SUM(CASE WHEN m.home_team_id = t.team_id THEN m.away_score ELSE m.home_score END) as goals_against
    FROM teams t
    JOIN matches m ON (m.home_team_id = t.team_id OR m.away_team_id = t.team_id)
    WHERE m.status = 'FINISHED' AND m.home_score IS NOT NULL
    GROUP BY t.team_id, t.name
    HAVING COUNT(m.match_id) > 5
""")

teams = cur.fetchall()
print(f"Updating strengths for {len(teams)} teams...\n")

for team in teams:
    played = team['played']
    gf = team['goals_for'] or 0
    ga = team['goals_against'] or 0
    
    attack = max(0.5, min(2.0, (gf / played) / 1.5))
    defense = max(0.5, min(2.0, (ga / played) / 1.5))
    
    cur.execute("UPDATE teams SET attack_strength = %s, defense_strength = %s WHERE team_id = %s",
               (attack, defense, team['team_id']))
    
    print(f"{team['name']}: Attack {attack:.2f}, Defense {defense:.2f}")

conn.commit()
print(f"\n✅ Updated {len(teams)} teams")

cur.close()
conn.close()
