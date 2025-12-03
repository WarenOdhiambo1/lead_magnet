#!/usr/bin/env python3
"""Test data ingestion pipeline"""

import os
from dotenv import load_dotenv
from allsports_client import AllSportsApiClient
from data_pipeline import DataPipeline

load_dotenv()

def main():
    print("=" * 80)
    print("🧪 TESTING DATA INGESTION PIPELINE")
    print("=" * 80)
    print()
    
    api_key = os.getenv("ALLSPORTSAPI_KEY")
    if not api_key:
        print("❌ ALLSPORTSAPI_KEY not found in .env")
        return
    
    print(f"✅ API Key loaded: {api_key[:20]}...")
    print()
    
    # Initialize client and pipeline
    client = AllSportsApiClient(api_key)
    pipeline = DataPipeline(client)
    
    # Test with Premier League
    league_id = 152
    league_name = "Premier League"
    country = "England"
    
    print(f"🎯 Target: {league_name} (ID: {league_id})")
    print()
    
    try:
        result = pipeline.run_full_ingestion(league_id, league_name, country)
        
        print()
        print("=" * 80)
        print("📊 INGESTION SUMMARY")
        print("=" * 80)
        print(f"League ID:  {result['league_id']}")
        print(f"Teams:      {result['teams']}")
        print(f"Standings:  {result['standings']}")
        print(f"Fixtures:   {result['fixtures']}")
        print("=" * 80)
        print()
        print("✅ Test completed successfully!")
        
    except Exception as e:
        print()
        print("=" * 80)
        print("❌ ERROR")
        print("=" * 80)
        print(str(e))
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
