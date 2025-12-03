-- ============================================================================
-- 🗑️ REMOVE CORRUPTED PLAYER DATA
-- ============================================================================
-- Player data is outdated (2022/2023) and has corrupted stats
-- We don't need it for match predictions - team strengths are sufficient
-- ============================================================================

\echo '================================================================================'
\echo '🗑️  REMOVING CORRUPTED PLAYER DATA'
\echo '================================================================================'
\echo ''

BEGIN;

\echo '1️⃣  Current player count:'
SELECT COUNT(*) as total_players FROM players;

\echo ''
\echo '2️⃣  Deleting ALL players (data is corrupted and outdated)...'
DELETE FROM players;

\echo ''
\echo '3️⃣  Verification - players remaining:'
SELECT COUNT(*) as remaining_players FROM players;

\echo ''
\echo '4️⃣  What we KEEP (still valid for predictions):'
SELECT 
    'Teams' as entity,
    COUNT(*) as count
FROM teams
WHERE name NOT LIKE '%U18%' 
  AND name NOT LIKE '%U21%'
  AND name NOT LIKE '% W'
UNION ALL
SELECT 'Matches', COUNT(*) FROM matches
UNION ALL
SELECT 'Market Odds', COUNT(*) FROM market_odds
UNION ALL
SELECT 'Referees', COUNT(*) FROM referees
UNION ALL
SELECT 'Bookmakers', COUNT(*) FROM bookmakers;

COMMIT;

\echo ''
\echo '================================================================================'
\echo '✅ CLEANUP COMPLETE'
\echo '================================================================================'
\echo ''
\echo '💡 WHAT WE HAVE NOW:'
\echo '   ✅ Real teams with accurate strengths'
\echo '   ✅ Real fixtures (past and future)'
\echo '   ✅ Real bookmaker odds'
\echo '   ✅ Real referees'
\echo '   ❌ NO corrupted player data'
\echo ''
\echo '💡 FOR PREDICTIONS WE NEED:'
\echo '   ✅ Team attack/defense strengths (WE HAVE THIS)'
\echo '   ✅ Historical match results (WE HAVE THIS)'
\echo '   ✅ Current odds (WE HAVE THIS)'
\echo '   ❌ Player data (NOT NEEDED for team-level predictions)'
\echo ''
