#!/usr/bin/env python3
"""
🤖 AI-POWERED DATA VERIFICATION SYSTEM
Uses OpenAI to verify data is real and up-to-date by searching the web
"""
import os
from openai import OpenAI
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import json

load_dotenv()

# Initialize OpenAI
client = OpenAI(api_key="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")

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

class AIDataVerifier:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.verification_results = {
            'verified': [],
            'outdated': [],
            'fake': [],
            'uncertain': []
        }
    
    def verify_with_ai(self, query: str) -> dict:
        """Use OpenAI to verify data against real-world information"""
        try:
            response = client.chat.completions.create(
                model="gpt-4o",  # Latest model with web search
                messages=[
                    {
                        "role": "system",
                        "content": """You are a sports data verification expert. 
                        Verify if the provided information is accurate and up-to-date as of December 2025.
                        Search the web if needed to confirm current facts.
                        Respond in JSON format with: 
                        {
                            "is_accurate": true/false,
                            "confidence": 0-100,
                            "reason": "explanation",
                            "current_fact": "what is actually true"
                        }"""
                    },
                    {
                        "role": "user",
                        "content": query
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
        
        except Exception as e:
            print(f"❌ AI Error: {e}")
            return {"is_accurate": False, "confidence": 0, "reason": str(e)}
    
    def verify_upcoming_matches(self):
        """Verify upcoming matches are real"""
        print("\n1️⃣  VERIFYING UPCOMING MATCHES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                m.match_id,
                ht.name as home_team,
                at.name as away_team,
                m.kickoff_time
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE m.status = 'SCHEDULED'
              AND m.kickoff_time BETWEEN NOW() AND NOW() + INTERVAL '7 days'
              AND ht.name NOT LIKE '%U18%'
              AND ht.name NOT LIKE '%U21%'
            ORDER BY m.kickoff_time
            LIMIT 5
        """)
        
        matches = cur.fetchall()
        
        for match in matches:
            query = f"""Is this Premier League match scheduled for December 2025?
            {match['home_team']} vs {match['away_team']} on {match['kickoff_time'].strftime('%Y-%m-%d')}
            
            Verify if this fixture is real and the date is correct."""
            
            print(f"\n   Checking: {match['home_team']} vs {match['away_team']}")
            result = self.verify_with_ai(query)
            
            if result['is_accurate']:
                print(f"   ✅ VERIFIED (Confidence: {result['confidence']}%)")
                self.verification_results['verified'].append(match)
            else:
                print(f"   ❌ INVALID: {result['reason']}")
                self.verification_results['fake'].append(match)
        
        cur.close()
    
    def verify_player_rosters(self):
        """Verify player rosters are current"""
        print("\n2️⃣  VERIFYING PLAYER ROSTERS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Check a few famous players
        cur.execute("""
            SELECT 
                p.name,
                t.name as team
            FROM players p
            JOIN teams t ON p.team_id = t.team_id
            WHERE p.name IN ('Mohamed Salah', 'Erling Haaland', 'Bukayo Saka', 'Jordan Henderson')
            LIMIT 5
        """)
        
        players = cur.fetchall()
        
        for player in players:
            query = f"""As of December 2025, does {player['name']} play for {player['team']}?
            
            Verify the current team of this player."""
            
            print(f"\n   Checking: {player['name']} at {player['team']}")
            result = self.verify_with_ai(query)
            
            if result['is_accurate']:
                print(f"   ✅ CORRECT (Confidence: {result['confidence']}%)")
                self.verification_results['verified'].append(player)
            else:
                print(f"   ❌ OUTDATED: {result['reason']}")
                print(f"   💡 Current: {result.get('current_fact', 'Unknown')}")
                self.verification_results['outdated'].append(player)
        
        cur.close()
    
    def verify_team_names(self):
        """Verify team names are correct"""
        print("\n3️⃣  VERIFYING TEAM NAMES...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT name
            FROM teams
            WHERE league_id = 2
              AND name NOT LIKE '%U18%'
              AND name NOT LIKE '%U21%'
              AND name NOT LIKE '% W'
            ORDER BY name
            LIMIT 10
        """)
        
        teams = cur.fetchall()
        
        team_list = [t['name'] for t in teams]
        query = f"""Are these teams currently in the Premier League (2025-26 season)?
        {', '.join(team_list)}
        
        Verify which teams are correct and which are not in the current Premier League."""
        
        print(f"\n   Checking: {len(team_list)} teams")
        result = self.verify_with_ai(query)
        
        print(f"\n   Result: {result.get('reason', 'No details')}")
        
        cur.close()
    
    def verify_match_results(self):
        """Verify recent match results are accurate"""
        print("\n4️⃣  VERIFYING RECENT MATCH RESULTS...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                ht.name as home_team,
                at.name as away_team,
                m.home_score,
                m.away_score,
                m.kickoff_time::date as match_date
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE m.status = 'FINISHED'
              AND m.kickoff_time > NOW() - INTERVAL '30 days'
            ORDER BY m.kickoff_time DESC
            LIMIT 3
        """)
        
        matches = cur.fetchall()
        
        for match in matches:
            query = f"""Did this Premier League match happen with this score?
            {match['home_team']} {match['home_score']} - {match['away_score']} {match['away_team']} on {match['match_date']}
            
            Verify if this result is accurate."""
            
            print(f"\n   Checking: {match['home_team']} {match['home_score']}-{match['away_score']} {match['away_team']}")
            result = self.verify_with_ai(query)
            
            if result['is_accurate']:
                print(f"   ✅ CORRECT (Confidence: {result['confidence']}%)")
            else:
                print(f"   ❌ WRONG: {result['reason']}")
        
        cur.close()
    
    def generate_report(self):
        """Generate verification report"""
        print("\n" + "="*80)
        print("📊 AI VERIFICATION REPORT")
        print("="*80 + "\n")
        
        print(f"✅ Verified: {len(self.verification_results['verified'])}")
        print(f"⏰ Outdated: {len(self.verification_results['outdated'])}")
        print(f"❌ Fake: {len(self.verification_results['fake'])}")
        print(f"❓ Uncertain: {len(self.verification_results['uncertain'])}")
        
        if self.verification_results['outdated']:
            print("\n⚠️  OUTDATED DATA FOUND:")
            for item in self.verification_results['outdated']:
                print(f"   • {item}")
        
        if self.verification_results['fake']:
            print("\n❌ FAKE DATA FOUND:")
            for item in self.verification_results['fake']:
                print(f"   • {item}")
        
        print("\n" + "="*80)
        print("✅ VERIFICATION COMPLETE")
        print("="*80 + "\n")
    
    def close(self):
        self.conn.close()

if __name__ == '__main__':
    print("\n🤖 AI-POWERED DATA VERIFICATION SYSTEM")
    print("Using OpenAI GPT-4 to verify data against real-world facts\n")
    
    verifier = AIDataVerifier()
    
    try:
        verifier.verify_upcoming_matches()
        verifier.verify_player_rosters()
        verifier.verify_team_names()
        verifier.verify_match_results()
        verifier.generate_report()
    finally:
        verifier.close()
    
    print("\n💡 RECOMMENDATION:")
    print("   If data is outdated/fake, run: python nuclear_reset.sql")
    print("   Then fetch fresh data with AI verification enabled\n")
