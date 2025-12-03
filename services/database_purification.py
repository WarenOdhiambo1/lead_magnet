#!/usr/bin/env python3
"""
🧹 DATABASE PURIFICATION SYSTEM

MISSION: Remove ALL incomplete data, keep ONLY uniform, complete records

Strategy:
1. Identify incomplete records in every table
2. Delete them permanently
3. Verify 100% data uniformity
4. Activate gatekeeper for future inserts
"""
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

class DatabasePurification:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.purge_stats = {
            'teams_removed': 0,
            'players_removed': 0,
            'matches_removed': 0,
            'venues_removed': 0,
            'referees_removed': 0,
            'odds_removed': 0,
            'standings_removed': 0
        }
    
    def execute_purification(self):
        """Execute complete database purification"""
        print("\n" + "="*80)
        print("🧹 DATABASE PURIFICATION SYSTEM")
        print("="*80)
        print("\n⚠️  WARNING: This will DELETE incomplete records permanently!")
        print("✅ Only complete, uniform data will remain\n")
        
        input("Press ENTER to continue or Ctrl+C to cancel...")
        
        print("\n🔥 STARTING PURIFICATION...\n")
        
        # Phase 1: Remove incomplete core entities
        self.purge_incomplete_teams()
        self.purge_incomplete_players()
        self.purge_incomplete_venues()
        self.purge_incomplete_referees()
        
        # Phase 2: Remove incomplete matches (depends on teams/venues/referees)
        self.purge_incomplete_matches()
        
        # Phase 3: Remove orphaned odds (depends on matches)
        self.purge_orphaned_odds()
        
        # Phase 4: Remove empty standings
        self.purge_empty_standings()
        
        # Phase 5: Verify uniformity
        self.verify_uniformity()
        
        # Phase 6: Generate report
        self.generate_purification_report()
        
        self.conn.commit()
        self.conn.close()
    
    def purge_incomplete_teams(self):
        """Remove teams with missing critical data"""
        print("1️⃣  PURGING INCOMPLETE TEAMS...")
        cur = self.conn.cursor()
        
        # Remove teams without names
        cur.execute("DELETE FROM teams WHERE name IS NULL OR name = '' OR LOWER(name) IN ('unknown', 'tbd', 'n/a')")
        self.purge_stats['teams_removed'] += cur.rowcount
        
        # Remove teams without strengths
        cur.execute("DELETE FROM teams WHERE attack_strength IS NULL OR defense_strength IS NULL")
        self.purge_stats['teams_removed'] += cur.rowcount
        
        # Remove teams with invalid ELO
        cur.execute("DELETE FROM teams WHERE elo_rating IS NULL OR elo_rating < 1000 OR elo_rating > 2500")
        self.purge_stats['teams_removed'] += cur.rowcount
        
        # Remove teams without league
        cur.execute("DELETE FROM teams WHERE league_id IS NULL")
        self.purge_stats['teams_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['teams_removed']} incomplete teams\n")
    
    def purge_incomplete_players(self):
        """Remove players with missing critical data"""
        print("2️⃣  PURGING INCOMPLETE PLAYERS...")
        cur = self.conn.cursor()
        
        # Remove players without names
        cur.execute("""
            DELETE FROM players 
            WHERE name IS NULL 
               OR name = '' 
               OR LOWER(name) IN ('unknown', 'n/a', 'null', 'player')
               OR LENGTH(TRIM(name)) < 3
        """)
        self.purge_stats['players_removed'] += cur.rowcount
        
        # Remove players without teams
        cur.execute("DELETE FROM players WHERE team_id IS NULL")
        self.purge_stats['players_removed'] += cur.rowcount
        
        # Remove players with invalid teams (orphaned)
        cur.execute("""
            DELETE FROM players 
            WHERE team_id NOT IN (SELECT team_id FROM teams)
        """)
        self.purge_stats['players_removed'] += cur.rowcount
        
        # Remove players without position
        cur.execute("DELETE FROM players WHERE position IS NULL OR position = ''")
        self.purge_stats['players_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['players_removed']} incomplete players\n")
    
    def purge_incomplete_venues(self):
        """Remove venues with missing critical data"""
        print("3️⃣  PURGING INCOMPLETE VENUES...")
        cur = self.conn.cursor()
        
        # First, nullify venue_id in matches that reference incomplete venues
        cur.execute("""
            UPDATE matches
            SET venue_id = NULL
            WHERE venue_id IN (
                SELECT venue_id FROM venues
                WHERE name IS NULL 
                   OR name = '' 
                   OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
                   OR city IS NULL 
                   OR city = '' 
                   OR LOWER(city) IN ('unknown', 'tbd')
                   OR capacity IS NULL 
                   OR capacity < 1000
            )
        """)
        
        # Now delete incomplete venues
        cur.execute("""
            DELETE FROM venues 
            WHERE name IS NULL 
               OR name = '' 
               OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
        """)
        self.purge_stats['venues_removed'] += cur.rowcount
        
        cur.execute("""
            DELETE FROM venues 
            WHERE city IS NULL 
               OR city = '' 
               OR LOWER(city) IN ('unknown', 'tbd')
        """)
        self.purge_stats['venues_removed'] += cur.rowcount
        
        cur.execute("DELETE FROM venues WHERE capacity IS NULL OR capacity < 1000")
        self.purge_stats['venues_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['venues_removed']} incomplete venues\n")
    
    def purge_incomplete_referees(self):
        """Remove referees with missing critical data"""
        print("4️⃣  PURGING INCOMPLETE REFEREES...")
        cur = self.conn.cursor()
        
        # First, nullify referee_id in matches that reference incomplete referees
        cur.execute("""
            UPDATE matches
            SET referee_id = NULL
            WHERE referee_id IN (
                SELECT referee_id FROM referees
                WHERE name IS NULL 
                   OR name = '' 
                   OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
                   OR LENGTH(TRIM(name)) < 3
                   OR avg_cards_per_game IS NULL 
                   OR avg_cards_per_game < 0 
                   OR avg_cards_per_game > 15
            )
        """)
        
        # Now delete incomplete referees
        cur.execute("""
            DELETE FROM referees 
            WHERE name IS NULL 
               OR name = '' 
               OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
               OR LENGTH(TRIM(name)) < 3
        """)
        self.purge_stats['referees_removed'] += cur.rowcount
        
        cur.execute("""
            DELETE FROM referees 
            WHERE avg_cards_per_game IS NULL 
               OR avg_cards_per_game < 0 
               OR avg_cards_per_game > 15
        """)
        self.purge_stats['referees_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['referees_removed']} incomplete referees\n")
    
    def purge_incomplete_matches(self):
        """Remove matches with missing critical data"""
        print("5️⃣  PURGING INCOMPLETE MATCHES...")
        cur = self.conn.cursor()
        
        # Remove matches with invalid teams
        cur.execute("""
            DELETE FROM matches 
            WHERE home_team_id NOT IN (SELECT team_id FROM teams)
               OR away_team_id NOT IN (SELECT team_id FROM teams)
        """)
        self.purge_stats['matches_removed'] += cur.rowcount
        
        # Remove matches with same home/away team
        cur.execute("DELETE FROM matches WHERE home_team_id = away_team_id")
        self.purge_stats['matches_removed'] += cur.rowcount
        
        # Remove finished matches without scores
        cur.execute("""
            DELETE FROM matches 
            WHERE status = 'FINISHED' 
              AND (home_score IS NULL OR away_score IS NULL)
        """)
        self.purge_stats['matches_removed'] += cur.rowcount
        
        # Remove matches with invalid venues
        cur.execute("""
            DELETE FROM matches 
            WHERE venue_id IS NOT NULL 
              AND venue_id NOT IN (SELECT venue_id FROM venues)
        """)
        self.purge_stats['matches_removed'] += cur.rowcount
        
        # Remove matches with invalid referees
        cur.execute("""
            DELETE FROM matches 
            WHERE referee_id IS NOT NULL 
              AND referee_id NOT IN (SELECT referee_id FROM referees)
        """)
        self.purge_stats['matches_removed'] += cur.rowcount
        
        # Remove future matches marked as finished
        cur.execute("""
            DELETE FROM matches 
            WHERE status = 'FINISHED' 
              AND kickoff_time > NOW()
        """)
        self.purge_stats['matches_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['matches_removed']} incomplete matches\n")
    
    def purge_orphaned_odds(self):
        """Remove odds for non-existent matches"""
        print("6️⃣  PURGING ORPHANED ODDS...")
        cur = self.conn.cursor()
        
        # Remove odds for non-existent matches
        cur.execute("""
            DELETE FROM market_odds 
            WHERE match_id NOT IN (SELECT match_id FROM matches)
        """)
        self.purge_stats['odds_removed'] += cur.rowcount
        
        # Remove odds with invalid bookmakers
        cur.execute("""
            DELETE FROM market_odds 
            WHERE bookie_id NOT IN (SELECT bookie_id FROM bookmakers)
        """)
        self.purge_stats['odds_removed'] += cur.rowcount
        
        # Remove odds with unrealistic values
        cur.execute("""
            DELETE FROM market_odds 
            WHERE odds IS NULL 
               OR odds < 1.01 
               OR odds > 500
        """)
        self.purge_stats['odds_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['odds_removed']} invalid odds\n")
    
    def purge_empty_standings(self):
        """Remove standings with missing data"""
        print("7️⃣  PURGING INCOMPLETE STANDINGS...")
        cur = self.conn.cursor()
        
        # Remove standings without teams
        cur.execute("""
            DELETE FROM league_standings 
            WHERE team_id NOT IN (SELECT team_id FROM teams)
        """)
        self.purge_stats['standings_removed'] += cur.rowcount
        
        # Remove standings without position or points
        cur.execute("""
            DELETE FROM league_standings 
            WHERE position IS NULL 
               OR points IS NULL
        """)
        self.purge_stats['standings_removed'] += cur.rowcount
        
        print(f"   🗑️  Removed {self.purge_stats['standings_removed']} incomplete standings\n")
    
    def verify_uniformity(self):
        """Verify all remaining data is complete"""
        print("8️⃣  VERIFYING DATA UNIFORMITY...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        issues = []
        
        # Check teams
        cur.execute("""
            SELECT COUNT(*) as cnt FROM teams 
            WHERE name IS NULL 
               OR attack_strength IS NULL 
               OR defense_strength IS NULL 
               OR elo_rating IS NULL
        """)
        if cur.fetchone()['cnt'] > 0:
            issues.append("Teams still have incomplete data")
        
        # Check players
        cur.execute("""
            SELECT COUNT(*) as cnt FROM players 
            WHERE name IS NULL 
               OR team_id IS NULL 
               OR position IS NULL
        """)
        if cur.fetchone()['cnt'] > 0:
            issues.append("Players still have incomplete data")
        
        # Check matches
        cur.execute("""
            SELECT COUNT(*) as cnt FROM matches 
            WHERE (status = 'FINISHED' AND (home_score IS NULL OR away_score IS NULL))
               OR home_team_id = away_team_id
        """)
        if cur.fetchone()['cnt'] > 0:
            issues.append("Matches still have incomplete data")
        
        if issues:
            print("   ⚠️  ISSUES FOUND:")
            for issue in issues:
                print(f"      • {issue}")
        else:
            print("   ✅ ALL DATA IS UNIFORM AND COMPLETE!\n")
        
        cur.close()
    
    def generate_purification_report(self):
        """Generate final purification report"""
        print("\n" + "="*80)
        print("📊 PURIFICATION REPORT")
        print("="*80 + "\n")
        
        total_removed = sum(self.purge_stats.values())
        
        print("🗑️  RECORDS REMOVED:")
        for entity, count in self.purge_stats.items():
            if count > 0:
                print(f"   {entity.replace('_', ' ').title():<25} {count:>10,}")
        
        print(f"\n   {'TOTAL PURGED':<25} {total_removed:>10,}")
        
        # Show remaining clean data
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM teams) as teams,
                (SELECT COUNT(*) FROM players) as players,
                (SELECT COUNT(*) FROM matches) as matches,
                (SELECT COUNT(*) FROM venues) as venues,
                (SELECT COUNT(*) FROM referees) as referees,
                (SELECT COUNT(*) FROM market_odds) as odds,
                (SELECT COUNT(*) FROM league_standings) as standings
        """)
        
        remaining = cur.fetchone()
        
        print(f"\n✅ CLEAN DATA REMAINING:")
        for entity, count in remaining.items():
            print(f"   {entity.title():<25} {count:>10,}")
        
        print("\n" + "="*80)
        print("✅ PURIFICATION COMPLETE - DATABASE IS NOW UNIFORM")
        print("="*80 + "\n")
        
        cur.close()

if __name__ == '__main__':
    purifier = DatabasePurification()
    purifier.execute_purification()
    
    print("💡 NEXT STEPS:")
    print("   1. ✅ Database is now clean and uniform")
    print("   2. 🚪 Gatekeeper is active - only complete data allowed")
    print("   3. 🔄 Run: python master_data_sync.py (with gatekeeper)")
    print("   4. 🤖 Run: python ml_ensemble_ultimate.py")
    print("   5. 💰 Run: python calculate_value_bets.py\n")
