#!/usr/bin/env python3
"""
ADVANCED PREDICTION MODEL
Factors: Team Form, Players, Venue, Referee, Head-to-Head, Injuries
Goal: Maximum accuracy for profitable betting
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import numpy as np
from scipy.stats import poisson

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def calculate_form_factor(team_id, cur, last_n=5):
    """Recent form: Last 5 matches"""
    cur.execute("""
        SELECT 
            CASE 
                WHEN m.home_team_id = %s THEN 
                    CASE WHEN m.home_score > m.away_score THEN 3
                         WHEN m.home_score = m.away_score THEN 1
                         ELSE 0 END
                ELSE 
                    CASE WHEN m.away_score > m.home_score THEN 3
                         WHEN m.away_score = m.home_score THEN 1
                         ELSE 0 END
            END as points
        FROM matches m
        WHERE (m.home_team_id = %s OR m.away_team_id = %s)
          AND m.status = 'FINISHED'
        ORDER BY m.kickoff_time DESC
        LIMIT %s
    """, (team_id, team_id, team_id, last_n))
    
    results = cur.fetchall()
    if not results:
        return 1.0
    
    points = sum(r['points'] for r in results)
    max_points = last_n * 3
    return 0.7 + (points / max_points) * 0.6  # Range: 0.7 to 1.3

def calculate_venue_factor(team_id, venue_id, cur):
    """Home venue advantage"""
    if not venue_id:
        return 1.0
    
    cur.execute("""
        SELECT 
            COUNT(*) as played,
            SUM(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as won
        FROM matches m
        WHERE m.home_team_id = %s AND m.venue_id = %s AND m.status = 'FINISHED'
    """, (team_id, venue_id))
    
    result = cur.fetchone()
    if result['played'] < 3:
        return 1.15  # Default home advantage
    
    win_rate = result['won'] / result['played']
    return 1.0 + (win_rate * 0.3)  # Range: 1.0 to 1.3

def calculate_referee_factor(team_id, referee_id, cur):
    """Referee bias (cards, penalties)"""
    if not referee_id:
        return 1.0
    
    cur.execute("""
        SELECT COUNT(*) as matches
        FROM matches m
        WHERE (m.home_team_id = %s OR m.away_team_id = %s)
          AND m.referee_id = %s AND m.status = 'FINISHED'
    """, (team_id, team_id, referee_id))
    
    result = cur.fetchone()
    if result['matches'] < 3:
        return 1.0
    
    # Neutral for now, can add card/penalty data later
    return 1.0

def calculate_h2h_factor(home_id, away_id, cur):
    """Head-to-head history"""
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE 
                WHEN m.home_team_id = %s AND m.home_score > m.away_score THEN 1
                WHEN m.away_team_id = %s AND m.away_score > m.home_score THEN 1
                ELSE 0 
            END) as home_wins
        FROM matches m
        WHERE ((m.home_team_id = %s AND m.away_team_id = %s)
           OR (m.home_team_id = %s AND m.away_team_id = %s))
          AND m.status = 'FINISHED'
    """, (home_id, home_id, home_id, away_id, away_id, home_id))
    
    result = cur.fetchone()
    if result['total'] < 3:
        return 1.0, 1.0
    
    home_win_rate = result['home_wins'] / result['total']
    return 0.9 + (home_win_rate * 0.2), 1.1 - (home_win_rate * 0.2)

def calculate_injury_factor(team_id, cur):
    """Key player injuries"""
    cur.execute("""
        SELECT COUNT(*) as injured
        FROM players p
        WHERE p.team_id = %s AND p.is_injured = true
    """, (team_id,))
    
    result = cur.fetchone()
    injured = result['injured']
    
    if injured == 0:
        return 1.0
    elif injured <= 2:
        return 0.95
    elif injured <= 4:
        return 0.90
    else:
        return 0.85

def predict_match_advanced(match_id, cur):
    """Advanced prediction with all factors"""
    
    # Get match details
    cur.execute("""
        SELECT 
            m.match_id, m.home_team_id, m.away_team_id, m.venue_id, m.referee_id,
            ht.name as home_team, ht.attack_strength as home_att, ht.defense_strength as home_def,
            at.name as away_team, at.attack_strength as away_att, at.defense_strength as away_def
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE m.match_id = %s
    """, (match_id,))
    
    match = cur.fetchone()
    if not match or not match['home_att']:
        return None
    
    # Base strengths
    home_att = float(match['home_att'])
    home_def = float(match['home_def'])
    away_att = float(match['away_att'])
    away_def = float(match['away_def'])
    
    # Calculate adjustment factors
    home_form = calculate_form_factor(match['home_team_id'], cur)
    away_form = calculate_form_factor(match['away_team_id'], cur)
    
    venue_factor = calculate_venue_factor(match['home_team_id'], match['venue_id'], cur)
    
    home_ref = calculate_referee_factor(match['home_team_id'], match['referee_id'], cur)
    away_ref = calculate_referee_factor(match['away_team_id'], match['referee_id'], cur)
    
    h2h_home, h2h_away = calculate_h2h_factor(match['home_team_id'], match['away_team_id'], cur)
    
    home_injury = calculate_injury_factor(match['home_team_id'], cur)
    away_injury = calculate_injury_factor(match['away_team_id'], cur)
    
    # Adjusted strengths
    home_att_adj = home_att * home_form * venue_factor * home_ref * h2h_home * home_injury
    home_def_adj = home_def / (home_form * venue_factor)
    
    away_att_adj = away_att * away_form * away_ref * h2h_away * away_injury
    away_def_adj = away_def / away_form
    
    # Expected goals
    league_avg = 1.5
    home_xg = league_avg * home_att_adj * away_def_adj
    away_xg = league_avg * away_att_adj * home_def_adj
    
    # Bound xG
    home_xg = max(0.3, min(4.0, home_xg))
    away_xg = max(0.3, min(4.0, away_xg))
    
    # Calculate probabilities using Poisson
    max_goals = 7
    prob_matrix = np.zeros((max_goals, max_goals))
    
    for i in range(max_goals):
        for j in range(max_goals):
            prob_matrix[i][j] = poisson.pmf(i, home_xg) * poisson.pmf(j, away_xg)
    
    # Dixon-Coles correlation adjustment
    rho = -0.13
    if home_xg < 1.5 and away_xg < 1.5:
        tau = lambda x, y, lam1, lam2, rho: 1 + rho * lam1 * lam2 if x == 0 and y == 0 else \
              1 - rho * lam1 if x == 0 and y == 1 else \
              1 - rho * lam2 if x == 1 and y == 0 else \
              1 + rho if x == 1 and y == 1 else 1
        
        for i in range(min(2, max_goals)):
            for j in range(min(2, max_goals)):
                prob_matrix[i][j] *= tau(i, j, home_xg, away_xg, rho)
    
    # Normalize
    prob_matrix /= prob_matrix.sum()
    
    # Calculate outcome probabilities
    prob_home = np.sum(np.tril(prob_matrix, -1))
    prob_draw = np.sum(np.diag(prob_matrix))
    prob_away = np.sum(np.triu(prob_matrix, 1))
    
    return {
        'match_id': match_id,
        'home_team': match['home_team'],
        'away_team': match['away_team'],
        'home_xg': float(home_xg),
        'away_xg': float(away_xg),
        'prob_home': float(prob_home),
        'prob_draw': float(prob_draw),
        'prob_away': float(prob_away),
        'factors': {
            'home_form': float(home_form),
            'away_form': float(away_form),
            'venue': float(venue_factor),
            'h2h_home': float(h2h_home),
            'h2h_away': float(h2h_away),
            'home_injury': float(home_injury),
            'away_injury': float(away_injury)
        }
    }

def generate_all_predictions():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get upcoming matches
    cur.execute("""
        SELECT match_id FROM matches 
        WHERE status = 'SCHEDULED' AND kickoff_time > NOW()
        ORDER BY kickoff_time
    """)
    
    matches = cur.fetchall()
    print(f"🔮 Generating ADVANCED predictions for {len(matches)} matches...\n")
    
    predictions = []
    for match in matches:
        pred = predict_match_advanced(match['match_id'], cur)
        if pred:
            predictions.append(pred)
            
            # Save to database
            cur.execute("""
                INSERT INTO predictions (match_id, prob_home, prob_draw, prob_away, model_version)
                VALUES (%s, %s, %s, %s, 'advanced_v1')
                ON CONFLICT (match_id) DO UPDATE 
                SET prob_home = EXCLUDED.prob_home,
                    prob_draw = EXCLUDED.prob_draw,
                    prob_away = EXCLUDED.prob_away,
                    model_version = EXCLUDED.model_version
            """, (pred['match_id'], pred['prob_home'], pred['prob_draw'], pred['prob_away']))
            
            print(f"{pred['home_team']} vs {pred['away_team']}")
            print(f"  xG: {pred['home_xg']:.2f} - {pred['away_xg']:.2f}")
            print(f"  Probabilities: {pred['prob_home']:.1%} | {pred['prob_draw']:.1%} | {pred['prob_away']:.1%}")
            print(f"  Factors: Form({pred['factors']['home_form']:.2f}/{pred['factors']['away_form']:.2f}) "
                  f"Venue({pred['factors']['venue']:.2f}) "
                  f"H2H({pred['factors']['h2h_home']:.2f}/{pred['factors']['h2h_away']:.2f})")
            print()
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"✅ Generated {len(predictions)} advanced predictions!")
    return predictions

if __name__ == '__main__':
    generate_all_predictions()
