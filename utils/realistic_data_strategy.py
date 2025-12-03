#!/usr/bin/env python3
"""
🎯 REALISTIC DATA ACQUISITION STRATEGY

TRUTH: We can't get odds for matches 1+ year in the future!
SOLUTION: Focus on REAL, ACTIONABLE data for CURRENT season

Strategy:
1. Fetch CURRENT/UPCOMING matches (next 7-14 days)
2. Get odds ONLY for matches with published bookmaker data
3. Use historical data for ML training
4. Generate predictions ONLY for matches with complete data
5. Implement data quality gates - NO PREDICTION without 100% data
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

THEODDS_KEY = os.getenv('THEODDS_API_KEY', '987e61b1ed5b257c09f256c95a55b966')
THEODDS_BASE = "https://api.the-odds-api.com/v4/sports/soccer_epl"

class RealisticDataStrategy:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
    
    def execute_strategy(self):
        """Execute realistic data acquisition"""
        print("\n" + "="*80)
        print("🎯 REALISTIC DATA ACQUISITION STRATEGY")
        print("="*80 + "\n")
        
        # Step 1: Clean future matches (we can't predict 1 year ahead!)
        print("1️⃣ CLEANING UNREALISTIC FUTURE MATCHES...")
        self.clean_far_future_matches()
        
        # Step 2: Focus on actionable timeframe
        print("\n2️⃣ IDENTIFYING ACTIONABLE MATCHES...")
        actionable = self.identify_actionable_matches()
        
        # Step 3: Fetch odds for actionable matches
        print(f"\n3️⃣ FETCHING ODDS FOR {actionable} ACTIONABLE MATCHES...")
        self.fetch_current_odds()
        
        # Step 4: Validate data completeness
        print("\n4️⃣ VALIDATING DATA COMPLETENESS...")
        self.validate_completeness()
        
        # Step 5: Mark prediction-ready matches
        print("\n5️⃣ MARKING PREDICTION-READY MATCHES...")
        self.mark_prediction_ready()
        
        # Step 6: Generate final report
        self.generate_strategy_report()
        
        self.conn.commit()
        self.conn.close()
    
    def clean_far_future_matches(self):
        """Remove matches too far in future (no odds available)"""
        cur = self.conn.cursor()
        
        # Keep only matches within next 30 days OR historical matches
        cur.execute("""
            DELETE FROM matches
            WHERE kickoff_time > NOW() + INTERVAL '30 days'
              AND status = 'SCHEDULED'
        """)
        
        deleted = cur.rowcount
        print(f"   🗑️  Removed {deleted:,} far-future matches (no odds available)")
        
        cur.close()
    
    def identify_actionable_matches(self):
        """Identify matches we can actually predict"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Matches in next 14 days
        cur.execute("""
            SELECT COUNT(*) as cnt
            FROM matches
            WHERE kickoff_time BETWEEN NOW() AND NOW() + INTERVAL '14 days'
              AND status = 'SCHEDULED'
        """)
        
        count = cur.fetchone()['cnt']
        print(f"   📅 Found {count} matches in next 14 days")
        
        cur.close()
        return count
    
    def fetch_current_odds(self):
        """Fetch odds for current/upcoming matches"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Check API quota first
            url = f"{THEODDS_BASE}/odds/?apiKey={THEODDS_KEY}&regions=uk&markets=h2h&oddsFormat=decimal"
            
            response = requests.get(url, timeout=30)
            
            if response.status_code == 401:
                print("   ❌ API Key invalid or expired")
                return
            elif response.status_code == 429:
                print("   ⚠️  API quota exceeded - wait before next call")
                return
            
            odds_data = response.json()
            
            if isinstance(odds_data, dict) and 'message' in odds_data:
                print(f"   ⚠️  API Message: {odds_data['message']}")
                return
            
            print(f"   ✅ Fetched odds for {len(odds_data)} live events")
            
            # Sync odds to database
            synced = 0
            for event in odds_data:
                home_team = event.get('home_team', '')
                away_team = event.get('away_team', '')
                
                # Find match
                cur.execute("""
                    SELECT m.match_id
                    FROM matches m
                    JOIN teams ht ON m.home_team_id = ht.team_id
                    JOIN teams at ON m.away_team_id = at.team_id
                    WHERE (LOWER(ht.name) LIKE %s OR LOWER(ht.name) LIKE %s)
                      AND (LOWER(at.name) LIKE %s OR LOWER(at.name) LIKE %s)
                      AND m.status = 'SCHEDULED'
                      AND m.kickoff_time > NOW()
                    LIMIT 1
                """, (
                    f'%{home_team.lower()[:8]}%',
                    f'%{home_team.split()[-1].lower()}%',
                    f'%{away_team.lower()[:8]}%',
                    f'%{away_team.split()[-1].lower()}%'
                ))
                
                match = cur.fetchone()
                
                if match:
                    match_id = match['match_id']
                    
                    for bookmaker in event.get('bookmakers', []):
                        bookie_name = bookmaker.get('title', 'Unknown')
                        
                        # Create bookmaker
                        cur.execute("""
                            INSERT INTO bookmakers (name, trust_rating)
                            VALUES (%s, 5)
                            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                            RETURNING bookie_id
                        """, (bookie_name,))
                        
                        bookie_id = cur.fetchone()['bookie_id']
                        
                        # Insert odds
                        for market in bookmaker.get('markets', []):
                            for outcome in market.get('outcomes', []):
                                cur.execute("""
                                    INSERT INTO market_odds (
                                        match_id, bookie_id, market_type, selection, odds
                                    )
                                    VALUES (%s, %s, 'h2h', %s, %s)
                                    ON CONFLICT (match_id, bookie_id, market_type, selection)
                                    DO UPDATE SET odds = EXCLUDED.odds, timestamp = NOW()
                                """, (match_id, bookie_id, outcome['name'], outcome['price']))
                        
                        synced += 1
            
            print(f"   ✅ Synced odds for {synced} matches")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        cur.close()
    
    def validate_completeness(self):
        """Validate data completeness for predictions"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                COUNT(DISTINCT m.match_id) as total_upcoming,
                COUNT(DISTINCT mo.match_id) as with_odds,
                COUNT(DISTINCT CASE WHEN t1.attack_strength IS NOT NULL 
                                     AND t2.attack_strength IS NOT NULL THEN m.match_id END) as with_team_stats
            FROM matches m
            JOIN teams t1 ON m.home_team_id = t1.team_id
            JOIN teams t2 ON m.away_team_id = t2.team_id
            LEFT JOIN market_odds mo ON m.match_id = mo.match_id
            WHERE m.status = 'SCHEDULED'
              AND m.kickoff_time BETWEEN NOW() AND NOW() + INTERVAL '14 days'
        """)
        
        stats = cur.fetchone()
        
        print(f"   📊 Upcoming matches: {stats['total_upcoming']}")
        print(f"   📊 With odds: {stats['with_odds']} ({stats['with_odds']/max(stats['total_upcoming'],1)*100:.1f}%)")
        print(f"   📊 With team stats: {stats['with_team_stats']} ({stats['with_team_stats']/max(stats['total_upcoming'],1)*100:.1f}%)")
        
        cur.close()
    
    def mark_prediction_ready(self):
        """Mark matches that have complete data for prediction"""
        cur = self.conn.cursor()
        
        # Add prediction_ready column if doesn't exist
        cur.execute("""
            ALTER TABLE matches 
            ADD COLUMN IF NOT EXISTS prediction_ready BOOLEAN DEFAULT FALSE
        """)
        
        # Mark matches with complete data
        cur.execute("""
            UPDATE matches m
            SET prediction_ready = TRUE
            WHERE m.status = 'SCHEDULED'
              AND m.kickoff_time BETWEEN NOW() AND NOW() + INTERVAL '14 days'
              AND EXISTS (
                  SELECT 1 FROM market_odds mo 
                  WHERE mo.match_id = m.match_id
              )
              AND EXISTS (
                  SELECT 1 FROM teams t1 
                  WHERE t1.team_id = m.home_team_id 
                    AND t1.attack_strength IS NOT NULL
              )
              AND EXISTS (
                  SELECT 1 FROM teams t2 
                  WHERE t2.team_id = m.away_team_id 
                    AND t2.attack_strength IS NOT NULL
              )
        """)
        
        ready = cur.rowcount
        print(f"   ✅ Marked {ready} matches as prediction-ready")
        
        cur.close()
    
    def generate_strategy_report(self):
        """Generate final strategy report"""
        print("\n" + "="*80)
        print("📊 DATA STRATEGY REPORT")
        print("="*80 + "\n")
        
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM matches WHERE status = 'FINISHED') as historical,
                (SELECT COUNT(*) FROM matches WHERE status = 'SCHEDULED') as upcoming,
                (SELECT COUNT(*) FROM matches WHERE prediction_ready = TRUE) as ready_for_prediction,
                (SELECT COUNT(DISTINCT match_id) FROM market_odds) as with_odds,
                (SELECT COUNT(*) FROM league_standings) as standings,
                (SELECT COUNT(*) FROM players WHERE minutes_played > 0) as active_players
        """)
        
        stats = cur.fetchone()
        
        print("✅ DATA INVENTORY:")
        print(f"   Historical Matches: {stats['historical']:,} (for ML training)")
        print(f"   Upcoming Matches: {stats['upcoming']:,}")
        print(f"   Prediction-Ready: {stats['ready_for_prediction']:,} ⭐")
        print(f"   Matches with Odds: {stats['with_odds']:,}")
        print(f"   League Standings: {stats['standings']:,}")
        print(f"   Active Players: {stats['active_players']:,}")
        
        if stats['ready_for_prediction'] > 0:
            print(f"\n🎯 BUSINESS STATUS: OPERATIONAL")
            print(f"   ✅ Can generate predictions for {stats['ready_for_prediction']} matches")
            print(f"   ✅ Can identify value bets")
            print(f"   ✅ ML model can train on {stats['historical']:,} historical matches")
        else:
            print(f"\n⚠️  BUSINESS STATUS: LIMITED")
            print(f"   ⚠️  No matches ready for prediction")
            print(f"   💡 Reason: Bookmakers haven't published odds yet")
            print(f"   💡 Solution: Check again closer to match day (2-3 days before)")
        
        print("\n" + "="*80)
        print("✅ STRATEGY EXECUTION COMPLETE")
        print("="*80 + "\n")
        
        cur.close()

if __name__ == '__main__':
    strategy = RealisticDataStrategy()
    strategy.execute_strategy()
    
    print("💡 NEXT ACTIONS:")
    print("   1. Train ML model: python ml_ensemble_ultimate.py")
    print("   2. Generate predictions: python ml_ensemble_ultimate.py --predict-only")
    print("   3. Find value bets: python calculate_value_bets.py")
    print("   4. Schedule daily sync: Run this script daily at 6 AM\n")
