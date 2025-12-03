#!/usr/bin/env python3
"""
ADVANCED DATA QUALITY & DEDUPLICATION MANAGER
- Remove duplicate records across all tables
- Validate data completeness and accuracy
- Fix inconsistencies and missing values
- Ensure referential integrity
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

class DataQualityManager:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.issues_found = defaultdict(int)
        self.fixes_applied = defaultdict(int)
    
    def run_full_cleanup(self):
        """Run complete data quality check and cleanup"""
        print("\n" + "="*70)
        print("🧹 DATA QUALITY & DEDUPLICATION MANAGER")
        print("="*70 + "\n")
        
        # Step 1: Remove duplicates
        self.deduplicate_teams()
        self.deduplicate_venues()
        self.deduplicate_referees()
        self.deduplicate_players()
        self.deduplicate_matches()
        
        # Step 2: Validate completeness
        self.validate_teams()
        self.validate_matches()
        self.validate_players()
        self.validate_referees()
        
        # Step 3: Fix inconsistencies
        self.fix_team_strengths()
        self.fix_match_statuses()
        self.fix_missing_venues()
        
        # Step 4: Remove orphaned records
        self.remove_orphaned_records()
        
        # Step 5: Generate report
        self.generate_report()
        
        self.conn.commit()
        self.conn.close()
    
    def deduplicate_teams(self):
        """Remove duplicate teams by name"""
        print("1️⃣  DEDUPLICATING TEAMS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Find duplicates (case-insensitive)
        cur.execute("""
            SELECT LOWER(name) as name_lower, COUNT(*) as cnt, ARRAY_AGG(team_id ORDER BY team_id) as ids
            FROM teams
            GROUP BY LOWER(name)
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        
        for dup in duplicates:
            keep_id = dup['ids'][0]  # Keep first one
            remove_ids = dup['ids'][1:]
            
            print(f"   🔄 Merging duplicate: {dup['name_lower']} ({len(dup['ids'])} copies)")
            
            # Update all references to point to kept record
            for table, col in [('matches', 'home_team_id'), ('matches', 'away_team_id'), 
                               ('players', 'team_id'), ('league_standings', 'team_id')]:
                for remove_id in remove_ids:
                    cur.execute(f"UPDATE {table} SET {col} = %s WHERE {col} = %s", (keep_id, remove_id))
            
            # Delete duplicates
            cur.execute("DELETE FROM teams WHERE team_id = ANY(%s)", (remove_ids,))
            self.fixes_applied['teams_deduplicated'] += len(remove_ids)
        
        print(f"   ✅ Removed {self.fixes_applied['teams_deduplicated']} duplicate teams\n")
        cur.close()
    
    def deduplicate_venues(self):
        """Remove duplicate venues"""
        print("2️⃣  DEDUPLICATING VENUES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT LOWER(name) as name_lower, COUNT(*) as cnt, ARRAY_AGG(venue_id ORDER BY venue_id) as ids
            FROM venues
            GROUP BY LOWER(name)
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        
        for dup in duplicates:
            keep_id = dup['ids'][0]
            remove_ids = dup['ids'][1:]
            
            print(f"   🔄 Merging venue: {dup['name_lower']}")
            
            # Update references
            for remove_id in remove_ids:
                cur.execute("UPDATE matches SET venue_id = %s WHERE venue_id = %s", (keep_id, remove_id))
                cur.execute("UPDATE teams SET venue_id = %s WHERE venue_id = %s", (keep_id, remove_id))
            
            cur.execute("DELETE FROM venues WHERE venue_id = ANY(%s)", (remove_ids,))
            self.fixes_applied['venues_deduplicated'] += len(remove_ids)
        
        print(f"   ✅ Removed {self.fixes_applied['venues_deduplicated']} duplicate venues\n")
        cur.close()
    
    def deduplicate_referees(self):
        """Remove duplicate referees"""
        print("3️⃣  DEDUPLICATING REFEREES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT LOWER(name) as name_lower, COUNT(*) as cnt, ARRAY_AGG(referee_id ORDER BY referee_id) as ids
            FROM referees
            GROUP BY LOWER(name)
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        
        for dup in duplicates:
            keep_id = dup['ids'][0]
            remove_ids = dup['ids'][1:]
            
            print(f"   🔄 Merging referee: {dup['name_lower']}")
            
            for remove_id in remove_ids:
                cur.execute("UPDATE matches SET referee_id = %s WHERE referee_id = %s", (keep_id, remove_id))
            
            cur.execute("DELETE FROM referees WHERE referee_id = ANY(%s)", (remove_ids,))
            self.fixes_applied['referees_deduplicated'] += len(remove_ids)
        
        print(f"   ✅ Removed {self.fixes_applied['referees_deduplicated']} duplicate referees\n")
        cur.close()
    
    def deduplicate_players(self):
        """Remove duplicate players"""
        print("4️⃣  DEDUPLICATING PLAYERS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Find duplicates by name and team
        cur.execute("""
            SELECT LOWER(name) as name_lower, team_id, COUNT(*) as cnt, 
                   ARRAY_AGG(player_id ORDER BY player_id) as ids
            FROM players
            WHERE name IS NOT NULL
            GROUP BY LOWER(name), team_id
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        
        for dup in duplicates:
            keep_id = dup['ids'][0]
            remove_ids = dup['ids'][1:]
            
            # Delete duplicates (players don't have many references)
            cur.execute("DELETE FROM players WHERE player_id = ANY(%s)", (remove_ids,))
            self.fixes_applied['players_deduplicated'] += len(remove_ids)
        
        print(f"   ✅ Removed {self.fixes_applied['players_deduplicated']} duplicate players\n")
        cur.close()
    
    def deduplicate_matches(self):
        """Remove duplicate matches"""
        print("5️⃣  DEDUPLICATING MATCHES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Find duplicates by teams and date
        cur.execute("""
            SELECT home_team_id, away_team_id, kickoff_time::date as match_date, 
                   COUNT(*) as cnt, ARRAY_AGG(match_id ORDER BY match_id) as ids
            FROM matches
            GROUP BY home_team_id, away_team_id, kickoff_time::date
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        
        for dup in duplicates:
            keep_id = dup['ids'][0]
            remove_ids = dup['ids'][1:]
            
            print(f"   🔄 Removing duplicate match (keeping ID {keep_id})")
            
            # Update references
            for remove_id in remove_ids:
                cur.execute("UPDATE predictions SET match_id = %s WHERE match_id = %s", (keep_id, remove_id))
                cur.execute("UPDATE market_odds SET match_id = %s WHERE match_id = %s", (keep_id, remove_id))
            
            cur.execute("DELETE FROM matches WHERE match_id = ANY(%s)", (remove_ids,))
            self.fixes_applied['matches_deduplicated'] += len(remove_ids)
        
        print(f"   ✅ Removed {self.fixes_applied['matches_deduplicated']} duplicate matches\n")
        cur.close()
    
    def validate_teams(self):
        """Validate team data completeness"""
        print("6️⃣  VALIDATING TEAMS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Check for missing critical data
        cur.execute("""
            SELECT COUNT(*) as cnt FROM teams 
            WHERE name IS NULL OR name = ''
        """)
        self.issues_found['teams_no_name'] = cur.fetchone()['cnt']
        
        cur.execute("""
            SELECT COUNT(*) as cnt FROM teams 
            WHERE attack_strength IS NULL OR defense_strength IS NULL
        """)
        self.issues_found['teams_no_strength'] = cur.fetchone()['cnt']
        
        cur.execute("""
            SELECT COUNT(*) as cnt FROM teams 
            WHERE elo_rating IS NULL OR elo_rating < 1000 OR elo_rating > 2500
        """)
        self.issues_found['teams_invalid_elo'] = cur.fetchone()['cnt']
        
        print(f"   📊 Teams without names: {self.issues_found['teams_no_name']}")
        print(f"   📊 Teams without strengths: {self.issues_found['teams_no_strength']}")
        print(f"   📊 Teams with invalid ELO: {self.issues_found['teams_invalid_elo']}\n")
        
        cur.close()
    
    def validate_matches(self):
        """Validate match data"""
        print("7️⃣  VALIDATING MATCHES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Finished matches without scores
        cur.execute("""
            SELECT COUNT(*) as cnt FROM matches 
            WHERE status = 'FINISHED' AND (home_score IS NULL OR away_score IS NULL)
        """)
        self.issues_found['matches_finished_no_score'] = cur.fetchone()['cnt']
        
        # Matches with same home/away team
        cur.execute("""
            SELECT COUNT(*) as cnt FROM matches 
            WHERE home_team_id = away_team_id
        """)
        self.issues_found['matches_same_teams'] = cur.fetchone()['cnt']
        
        # Future matches marked as finished
        cur.execute("""
            SELECT COUNT(*) as cnt FROM matches 
            WHERE status = 'FINISHED' AND kickoff_time > NOW()
        """)
        self.issues_found['matches_future_finished'] = cur.fetchone()['cnt']
        
        print(f"   📊 Finished matches without scores: {self.issues_found['matches_finished_no_score']}")
        print(f"   📊 Matches with same teams: {self.issues_found['matches_same_teams']}")
        print(f"   📊 Future matches marked finished: {self.issues_found['matches_future_finished']}\n")
        
        cur.close()
    
    def validate_players(self):
        """Validate player data"""
        print("8️⃣  VALIDATING PLAYERS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT COUNT(*) as cnt FROM players WHERE name IS NULL OR name = ''")
        self.issues_found['players_no_name'] = cur.fetchone()['cnt']
        
        cur.execute("SELECT COUNT(*) as cnt FROM players WHERE team_id IS NULL")
        self.issues_found['players_no_team'] = cur.fetchone()['cnt']
        
        print(f"   📊 Players without names: {self.issues_found['players_no_name']}")
        print(f"   📊 Players without teams: {self.issues_found['players_no_team']}\n")
        
        cur.close()
    
    def validate_referees(self):
        """Validate referee data"""
        print("9️⃣  VALIDATING REFEREES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT COUNT(*) as cnt FROM referees WHERE name IS NULL OR name = ''")
        self.issues_found['referees_no_name'] = cur.fetchone()['cnt']
        
        print(f"   📊 Referees without names: {self.issues_found['referees_no_name']}\n")
        
        cur.close()
    
    def fix_team_strengths(self):
        """Fix missing or invalid team strengths"""
        print("🔧 FIXING TEAM STRENGTHS...")
        cur = self.conn.cursor()
        
        # Set default strengths for teams without them
        cur.execute("""
            UPDATE teams 
            SET attack_strength = 1.0, defense_strength = 1.0
            WHERE attack_strength IS NULL OR defense_strength IS NULL
        """)
        fixed = cur.rowcount
        
        # Fix invalid ELO ratings
        cur.execute("""
            UPDATE teams 
            SET elo_rating = 1500
            WHERE elo_rating IS NULL OR elo_rating < 1000 OR elo_rating > 2500
        """)
        fixed += cur.rowcount
        
        self.fixes_applied['team_strengths_fixed'] = fixed
        print(f"   ✅ Fixed {fixed} team strength/ELO issues\n")
        
        cur.close()
    
    def fix_match_statuses(self):
        """Fix incorrect match statuses"""
        print("🔧 FIXING MATCH STATUSES...")
        cur = self.conn.cursor()
        
        # Future matches should not be FINISHED
        cur.execute("""
            UPDATE matches 
            SET status = 'SCHEDULED'
            WHERE status = 'FINISHED' AND kickoff_time > NOW()
        """)
        fixed = cur.rowcount
        
        # Past matches with scores should be FINISHED
        cur.execute("""
            UPDATE matches 
            SET status = 'FINISHED'
            WHERE status != 'FINISHED' 
              AND kickoff_time < NOW() - INTERVAL '2 hours'
              AND home_score IS NOT NULL 
              AND away_score IS NOT NULL
        """)
        fixed += cur.rowcount
        
        self.fixes_applied['match_statuses_fixed'] = fixed
        print(f"   ✅ Fixed {fixed} match status issues\n")
        
        cur.close()
    
    def fix_missing_venues(self):
        """Create default venue for matches without one"""
        print("🔧 FIXING MISSING VENUES...")
        cur = self.conn.cursor()
        
        # Create "Unknown Venue" if it doesn't exist
        cur.execute("""
            INSERT INTO venues (name, city, capacity)
            VALUES ('Unknown Venue', 'Unknown', 0)
            ON CONFLICT DO NOTHING
            RETURNING venue_id
        """)
        result = cur.fetchone()
        
        if result:
            unknown_venue_id = result[0]
        else:
            cur.execute("SELECT venue_id FROM venues WHERE name = 'Unknown Venue'")
            unknown_venue_id = cur.fetchone()[0]
        
        # Assign to matches without venue
        cur.execute("""
            UPDATE matches 
            SET venue_id = %s
            WHERE venue_id IS NULL
        """, (unknown_venue_id,))
        
        self.fixes_applied['venues_fixed'] = cur.rowcount
        print(f"   ✅ Fixed {cur.rowcount} matches without venues\n")
        
        cur.close()
    
    def remove_orphaned_records(self):
        """Remove records that reference non-existent entities"""
        print("🗑️  REMOVING ORPHANED RECORDS...")
        cur = self.conn.cursor()
        
        # Players without valid teams
        cur.execute("""
            DELETE FROM players 
            WHERE team_id NOT IN (SELECT team_id FROM teams)
        """)
        orphaned = cur.rowcount
        
        # Matches with invalid teams
        cur.execute("""
            DELETE FROM matches 
            WHERE home_team_id NOT IN (SELECT team_id FROM teams)
               OR away_team_id NOT IN (SELECT team_id FROM teams)
        """)
        orphaned += cur.rowcount
        
        # Predictions for non-existent matches
        cur.execute("""
            DELETE FROM predictions 
            WHERE match_id NOT IN (SELECT match_id FROM matches)
        """)
        orphaned += cur.rowcount
        
        self.fixes_applied['orphaned_removed'] = orphaned
        print(f"   ✅ Removed {orphaned} orphaned records\n")
        
        cur.close()
    
    def generate_report(self):
        """Generate final data quality report"""
        print("\n" + "="*70)
        print("📊 DATA QUALITY REPORT")
        print("="*70 + "\n")
        
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Count records in each table
        tables = ['teams', 'venues', 'referees', 'players', 'matches', 'predictions', 'market_odds']
        
        print("📈 RECORD COUNTS:")
        for table in tables:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            count = cur.fetchone()['cnt']
            print(f"   {table.upper():<20} {count:>10,}")
        
        print("\n🔧 FIXES APPLIED:")
        for fix, count in self.fixes_applied.items():
            if count > 0:
                print(f"   {fix.replace('_', ' ').title():<30} {count:>10}")
        
        print("\n⚠️  REMAINING ISSUES:")
        has_issues = False
        for issue, count in self.issues_found.items():
            if count > 0:
                has_issues = True
                print(f"   {issue.replace('_', ' ').title():<30} {count:>10}")
        
        if not has_issues:
            print("   ✅ No critical issues found!")
        
        # Data quality score
        total_issues = sum(self.issues_found.values())
        total_fixes = sum(self.fixes_applied.values())
        
        print(f"\n🎯 DATA QUALITY SCORE:")
        print(f"   Total Issues Found: {total_issues}")
        print(f"   Total Fixes Applied: {total_fixes}")
        
        if total_issues == 0:
            print(f"   Quality Score: 100% ✅")
        else:
            score = max(0, 100 - (total_issues * 2))
            print(f"   Quality Score: {score}%")
        
        print("\n" + "="*70)
        print("✅ DATA QUALITY CHECK COMPLETE")
        print("="*70 + "\n")
        
        cur.close()

if __name__ == '__main__':
    manager = DataQualityManager()
    manager.run_full_cleanup()
    print("💡 Next step: python update_team_strengths.py")
    print("💡 Then run: python ml_ensemble_ultimate.py\n")
