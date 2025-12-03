-- ============================================================================
-- 💣 NUCLEAR RESET - DELETE EVERYTHING
-- ============================================================================
-- Complete database wipe - removes ALL data from ALL tables
-- ============================================================================

\echo '================================================================================'
\echo '💣 NUCLEAR RESET - DELETING EVERYTHING'
\echo '================================================================================'
\echo ''

BEGIN;

\echo '1️⃣  Current data inventory:'
SELECT 
    'Teams' as table_name,
    COUNT(*) as records
FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Matches', COUNT(*) FROM matches
UNION ALL
SELECT 'Market Odds', COUNT(*) FROM market_odds
UNION ALL
SELECT 'Venues', COUNT(*) FROM venues
UNION ALL
SELECT 'Referees', COUNT(*) FROM referees
UNION ALL
SELECT 'Bookmakers', COUNT(*) FROM bookmakers
UNION ALL
SELECT 'League Standings', COUNT(*) FROM league_standings
UNION ALL
SELECT 'Predictions', COUNT(*) FROM predictions
UNION ALL
SELECT 'Opportunities', COUNT(*) FROM opportunities;

\echo ''
\echo '2️⃣  DELETING ALL DATA...'

-- Delete in correct order (respecting foreign keys)
DELETE FROM opportunities;
DELETE FROM predictions;
DELETE FROM market_odds;
DELETE FROM league_standings;
DELETE FROM players;
DELETE FROM matches;
DELETE FROM bookmakers;
DELETE FROM referees;
DELETE FROM venues;
DELETE FROM teams;

-- Fix leagues foreign key constraint first
UPDATE leagues SET current_season_id = NULL;

-- Now delete seasons and leagues
DELETE FROM seasons;
DELETE FROM leagues;

\echo ''
\echo '3️⃣  Verification - all tables should be empty:'
SELECT 
    'Teams' as table_name,
    COUNT(*) as records
FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Matches', COUNT(*) FROM matches
UNION ALL
SELECT 'Market Odds', COUNT(*) FROM market_odds
UNION ALL
SELECT 'Venues', COUNT(*) FROM venues
UNION ALL
SELECT 'Referees', COUNT(*) FROM referees
UNION ALL
SELECT 'Bookmakers', COUNT(*) FROM bookmakers
UNION ALL
SELECT 'League Standings', COUNT(*) FROM league_standings
UNION ALL
SELECT 'Predictions', COUNT(*) FROM predictions
UNION ALL
SELECT 'Opportunities', COUNT(*) FROM opportunities;

COMMIT;

\echo ''
\echo '================================================================================'
\echo '✅ NUCLEAR RESET COMPLETE - DATABASE IS NOW EMPTY'
\echo '================================================================================'
\echo ''
\echo '💡 NEXT STEPS:'
\echo '   1. Run: python rebuild_database.py (fetch fresh data)'
\echo '   2. Run: python data_quality_manager.py (clean it)'
\echo '   3. Run: python update_team_strengths.py (calculate strengths)'
\echo '   4. Run: python ml_ensemble_ultimate.py (train model)'
\echo ''
