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

def calculate_value_bets():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get matches with predictions and odds
    cur.execute("""
        SELECT
            m.match_id,
            ht.name as home_team,
            at.name as away_team,
            p.prob_home,
            p.prob_draw,
            p.prob_away,
            m.kickoff_time
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        JOIN predictions p ON m.match_id = p.match_id
        WHERE EXISTS (
            SELECT 1 FROM market_odds mo 
            WHERE mo.match_id = m.match_id
        )
        ORDER BY m.kickoff_time
    """)
    matches = cur.fetchall()
    
    print(f"Analyzing {len(matches)} matches with predictions and odds...\n")
    print("=" * 100)
    
    value_bets = []
    
    for match in matches:
        match_id = match['match_id']
        home = match['home_team']
        away = match['away_team']
        
        # Model probabilities
        prob_home = float(match['prob_home'] or 0)
        prob_draw = float(match['prob_draw'] or 0)
        prob_away = float(match['prob_away'] or 0)
        
        if prob_home == 0 or prob_draw == 0 or prob_away == 0:
            continue
        
        # Fair odds (no margin)
        fair_odds_home = 1 / prob_home
        fair_odds_draw = 1 / prob_draw
        fair_odds_away = 1 / prob_away
        
        print(f"\n{home} vs {away}")
        print(f"Model: Home {prob_home:.1%} | Draw {prob_draw:.1%} | Away {prob_away:.1%}")
        print(f"Fair Odds: {fair_odds_home:.2f} | {fair_odds_draw:.2f} | {fair_odds_away:.2f}")
        
        # Get best odds from bookmakers
        cur.execute("""
            SELECT 
                selection,
                MAX(odds) as best_odds,
                COUNT(DISTINCT bookie_id) as num_bookies
            FROM market_odds
            WHERE match_id = %s AND market_type = 'h2h'
            GROUP BY selection
        """, (match_id,))
        odds_data = cur.fetchall()
        
        for odd in odds_data:
            selection = odd['selection']
            best_odds = float(odd['best_odds'])
            num_bookies = odd['num_bookies']
            
            # Determine model probability for this selection
            if selection == home:
                model_prob = prob_home
                fair_odds = fair_odds_home
            elif selection == away:
                model_prob = prob_away
                fair_odds = fair_odds_away
            elif selection == 'Draw':
                model_prob = prob_draw
                fair_odds = fair_odds_draw
            else:
                continue
            
            # Calculate expected value
            ev = (best_odds * model_prob) - 1
            ev_pct = ev * 100
            
            # Value bet if EV > 5%
            if ev_pct > 5:
                value_bets.append({
                    'match': f"{home} vs {away}",
                    'selection': selection,
                    'model_prob': model_prob,
                    'fair_odds': fair_odds,
                    'best_odds': best_odds,
                    'ev_pct': ev_pct,
                    'num_bookies': num_bookies
                })
                
                # Insert into opportunities table
                cur.execute("""
                    INSERT INTO opportunities (
                        match_id, opportunity_type, selection, 
                        model_prob, best_odds, expected_value, 
                        recommended_stake, timestamp
                    ) VALUES (%s, 'VALUE_BET', %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                """, (match_id, selection, model_prob, best_odds, ev, 100))
                
                print(f"  🎯 VALUE BET: {selection} @ {best_odds:.2f} (EV: {ev_pct:+.1f}%) [{num_bookies} bookies]")
            else:
                print(f"  {selection}: {best_odds:.2f} (EV: {ev_pct:+.1f}%)")
    
    conn.commit()
    
    print("\n" + "=" * 100)
    print(f"\n✅ Analysis Complete: Found {len(value_bets)} value bets")
    
    if value_bets:
        print("\n📊 VALUE BET SUMMARY:")
        print("-" * 100)
        for bet in sorted(value_bets, key=lambda x: x['ev_pct'], reverse=True):
            print(f"{bet['match']}")
            print(f"  Bet: {bet['selection']} @ {bet['best_odds']:.2f}")
            print(f"  Model: {bet['model_prob']:.1%} (Fair: {bet['fair_odds']:.2f})")
            print(f"  EV: {bet['ev_pct']:+.1f}% | Bookies: {bet['num_bookies']}")
            print()
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    calculate_value_bets()
