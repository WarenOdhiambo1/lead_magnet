#!/usr/bin/env python3
"""Generate Dixon-Coles predictions for all matches"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import numpy as np
from scipy.stats import poisson

load_dotenv()

def dixon_coles_tau(home_goals, away_goals, rho=-0.13):
    """Dixon-Coles correlation adjustment"""
    if home_goals == 0 and away_goals == 0:
        return 1 - rho
    if home_goals == 0 and away_goals == 1:
        return 1 + rho
    if home_goals == 1 and away_goals == 0:
        return 1 + rho
    if home_goals == 1 and away_goals == 1:
        return 1 - rho
    return 1

def predict_match(lambda_home, lambda_away, max_goals=10):
    """Calculate match probabilities using Dixon-Coles"""
    matrix = np.zeros((max_goals + 1, max_goals + 1))
    
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            prob = poisson.pmf(h, lambda_home) * poisson.pmf(a, lambda_away)
            tau = dixon_coles_tau(h, a)
            matrix[h, a] = prob * tau
    
    matrix /= matrix.sum()
    
    prob_home = sum(matrix[h, a] for h in range(max_goals + 1) for a in range(h))
    prob_draw = sum(matrix[h, h] for h in range(max_goals + 1))
    prob_away = sum(matrix[h, a] for h in range(max_goals + 1) for a in range(h + 1, max_goals + 1))
    
    return {
        'prob_home': prob_home,
        'prob_draw': prob_draw,
        'prob_away': prob_away,
        'xg_home': lambda_home,
        'xg_away': lambda_away
    }

def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "student_finance_dream"),
        user=os.getenv("DB_USER", "Waren_Dev"),
        password=os.getenv("DB_PASSWORD", "")
    )
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("🔮 Generating Dixon-Coles predictions...")
    
    # Get matches without predictions
    cur.execute("""
        SELECT m.match_id, m.home_team_id, m.away_team_id,
               ht.attack_strength as home_attack,
               ht.defense_strength as home_defense,
               at.attack_strength as away_attack,
               at.defense_strength as away_defense,
               ht.name as home_team,
               at.name as away_team
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        LEFT JOIN predictions p ON m.match_id = p.match_id
        WHERE p.pred_id IS NULL
          AND m.status = 'Not Started'
    """)
    
    matches = cur.fetchall()
    print(f"Found {len(matches)} matches to predict\n")
    
    for match in matches:
        # Calculate expected goals with bounds
        league_avg = 1.5
        home_advantage = 1.15
        
        # Ensure strengths are positive and reasonable
        home_attack = max(0.5, min(2.0, float(match['home_attack'] or 1.0)))
        home_defense = max(0.5, min(2.0, float(match['home_defense'] or 1.0)))
        away_attack = max(0.5, min(2.0, float(match['away_attack'] or 1.0)))
        away_defense = max(0.5, min(2.0, float(match['away_defense'] or 1.0)))
        
        lambda_home = home_attack * away_defense * league_avg * home_advantage
        lambda_away = away_attack * home_defense * league_avg
        
        # Ensure positive xG
        lambda_home = max(0.1, lambda_home)
        lambda_away = max(0.1, lambda_away)
        
        # Generate prediction
        pred = predict_match(lambda_home, lambda_away)
        
        # Insert prediction
        cur.execute("""
            INSERT INTO predictions (match_id, prob_home, prob_draw, prob_away, xg_home, xg_away)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            match['match_id'],
            float(pred['prob_home']),
            float(pred['prob_draw']),
            float(pred['prob_away']),
            float(pred['xg_home']),
            float(pred['xg_away'])
        ))
        
        print(f"✅ {match['home_team']} vs {match['away_team']}")
        print(f"   Home: {pred['prob_home']*100:.1f}% | Draw: {pred['prob_draw']*100:.1f}% | Away: {pred['prob_away']*100:.1f}%")
        print(f"   xG: {pred['xg_home']:.2f} - {pred['xg_away']:.2f}\n")
    
    conn.commit()
    print(f"\n✅ Generated {len(matches)} predictions!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
