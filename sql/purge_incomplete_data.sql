-- ============================================================================
-- 🧹 DATABASE PURIFICATION - DIRECT SQL APPROACH
-- ============================================================================
-- This script removes ALL incomplete data directly from PostgreSQL
-- Only complete, uniform records will remain
-- ============================================================================

\echo '================================================================================'
\echo '🧹 STARTING DATABASE PURIFICATION'
\echo '================================================================================'
\echo ''

-- Start transaction
BEGIN;

\echo '1️⃣  NULLIFYING FOREIGN KEYS FOR INCOMPLETE VENUES...'
-- Nullify venue_id in MATCHES that reference incomplete venues
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
);

-- Nullify venue_id in TEAMS that reference incomplete venues
UPDATE teams
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
);

\echo '2️⃣  NULLIFYING FOREIGN KEYS FOR INCOMPLETE REFEREES...'
-- Nullify referee_id in matches that reference incomplete referees
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
);

\echo '3️⃣  DELETING INCOMPLETE VENUES...'
-- Delete incomplete venues
DELETE FROM venues 
WHERE name IS NULL 
   OR name = '' 
   OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
   OR city IS NULL 
   OR city = '' 
   OR LOWER(city) IN ('unknown', 'tbd')
   OR capacity IS NULL 
   OR capacity < 1000;

\echo '4️⃣  DELETING INCOMPLETE REFEREES...'
-- Delete incomplete referees
DELETE FROM referees 
WHERE name IS NULL 
   OR name = '' 
   OR LOWER(name) IN ('unknown', 'tbd', 'n/a')
   OR LENGTH(TRIM(name)) < 3
   OR avg_cards_per_game IS NULL 
   OR avg_cards_per_game < 0 
   OR avg_cards_per_game > 15;

\echo '5️⃣  DELETING INCOMPLETE TEAMS...'
-- Delete teams without names
DELETE FROM teams 
WHERE name IS NULL 
   OR name = '' 
   OR LOWER(name) IN ('unknown', 'tbd', 'n/a');

-- Delete teams without strengths
DELETE FROM teams 
WHERE attack_strength IS NULL 
   OR defense_strength IS NULL;

-- Delete teams with invalid ELO
DELETE FROM teams 
WHERE elo_rating IS NULL 
   OR elo_rating < 1000 
   OR elo_rating > 2500;

-- Delete teams without league
DELETE FROM teams 
WHERE league_id IS NULL;

\echo '6️⃣  DELETING INCOMPLETE PLAYERS...'
-- Delete players without names
DELETE FROM players 
WHERE name IS NULL 
   OR name = '' 
   OR LOWER(name) IN ('unknown', 'n/a', 'null', 'player')
   OR LENGTH(TRIM(name)) < 3;

-- Delete players without teams
DELETE FROM players 
WHERE team_id IS NULL;

-- Delete orphaned players (team doesn't exist)
DELETE FROM players 
WHERE team_id NOT IN (SELECT team_id FROM teams);

-- Delete players without position
DELETE FROM players 
WHERE position IS NULL 
   OR position = '';

\echo '7️⃣  DELETING INCOMPLETE MATCHES...'
-- Delete matches with invalid teams
DELETE FROM matches 
WHERE home_team_id NOT IN (SELECT team_id FROM teams)
   OR away_team_id NOT IN (SELECT team_id FROM teams);

-- Delete matches with same home/away team
DELETE FROM matches 
WHERE home_team_id = away_team_id;

-- Delete finished matches without scores
DELETE FROM matches 
WHERE status = 'FINISHED' 
  AND (home_score IS NULL OR away_score IS NULL);

-- Delete future matches marked as finished
DELETE FROM matches 
WHERE status = 'FINISHED' 
  AND kickoff_time > NOW();

\echo '8️⃣  DELETING ORPHANED ODDS...'
-- Delete odds for non-existent matches
DELETE FROM market_odds 
WHERE match_id NOT IN (SELECT match_id FROM matches);

-- Delete odds with invalid bookmakers
DELETE FROM market_odds 
WHERE bookie_id NOT IN (SELECT bookie_id FROM bookmakers);

-- Delete odds with unrealistic values
DELETE FROM market_odds 
WHERE odds IS NULL 
   OR odds < 1.01 
   OR odds > 500;

\echo '9️⃣  DELETING INCOMPLETE STANDINGS...'
-- Delete standings for non-existent teams
DELETE FROM league_standings 
WHERE team_id NOT IN (SELECT team_id FROM teams);

-- Delete standings without position or points
DELETE FROM league_standings 
WHERE position IS NULL 
   OR points IS NULL;

\echo '🔟 DELETING ORPHANED PREDICTIONS...'
-- Delete predictions for non-existent matches
DELETE FROM predictions 
WHERE match_id NOT IN (SELECT match_id FROM matches);

\echo ''
\echo '================================================================================'
\echo '📊 PURIFICATION SUMMARY'
\echo '================================================================================'

-- Show remaining clean data
SELECT 
    'Teams' as entity,
    COUNT(*) as remaining
FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Matches', COUNT(*) FROM matches
UNION ALL
SELECT 'Venues', COUNT(*) FROM venues
UNION ALL
SELECT 'Referees', COUNT(*) FROM referees
UNION ALL
SELECT 'Market Odds', COUNT(*) FROM market_odds
UNION ALL
SELECT 'Standings', COUNT(*) FROM league_standings
UNION ALL
SELECT 'Predictions', COUNT(*) FROM predictions
ORDER BY entity;

\echo ''
\echo '================================================================================'
\echo '✅ VERIFYING DATA UNIFORMITY'
\echo '================================================================================'

-- Verify no incomplete teams remain
SELECT 
    'Incomplete Teams' as check_type,
    COUNT(*) as count
FROM teams 
WHERE name IS NULL 
   OR attack_strength IS NULL 
   OR defense_strength IS NULL 
   OR elo_rating IS NULL
UNION ALL
-- Verify no incomplete players remain
SELECT 
    'Incomplete Players',
    COUNT(*)
FROM players 
WHERE name IS NULL 
   OR team_id IS NULL 
   OR position IS NULL
UNION ALL
-- Verify no incomplete matches remain
SELECT 
    'Incomplete Matches',
    COUNT(*)
FROM matches 
WHERE (status = 'FINISHED' AND (home_score IS NULL OR away_score IS NULL))
   OR home_team_id = away_team_id
UNION ALL
-- Verify no orphaned records
SELECT 
    'Orphaned Players',
    COUNT(*)
FROM players 
WHERE team_id NOT IN (SELECT team_id FROM teams)
UNION ALL
SELECT 
    'Orphaned Odds',
    COUNT(*)
FROM market_odds 
WHERE match_id NOT IN (SELECT match_id FROM matches);

\echo ''
\echo '================================================================================'
\echo '✅ PURIFICATION COMPLETE - COMMITTING CHANGES'
\echo '================================================================================'

-- Commit transaction
COMMIT;

\echo ''
\echo '💡 NEXT STEPS:'
\echo '   1. Database is now clean and uniform'
\echo '   2. Activate gatekeeper for future inserts'
\echo '   3. Run: python update_team_strengths.py'
\echo '   4. Run: python ml_ensemble_ultimate.py'
\echo ''
