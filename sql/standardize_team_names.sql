-- ============================================================================
-- 🔧 TEAM NAME STANDARDIZATION
-- ============================================================================
-- Merge duplicate teams with different name variations
-- ============================================================================

\echo '================================================================================'
\echo '🔧 TEAM NAME STANDARDIZATION'
\echo '================================================================================'
\echo ''

BEGIN;

\echo '1️⃣  IDENTIFYING DUPLICATE TEAMS...'
SELECT 
    team_id,
    name,
    COUNT(*) OVER (PARTITION BY 
        CASE 
            WHEN LOWER(name) LIKE '%leicester%' THEN 'Leicester City'
            WHEN LOWER(name) LIKE '%tottenham%' THEN 'Tottenham Hotspur'
            WHEN LOWER(name) LIKE '%brighton%' THEN 'Brighton & Hove Albion'
            WHEN LOWER(name) LIKE '%newcastle%' THEN 'Newcastle United'
            WHEN LOWER(name) LIKE '%manchester united%' OR LOWER(name) LIKE '%manchester utd%' THEN 'Manchester United'
            WHEN LOWER(name) LIKE '%west ham%' THEN 'West Ham United'
            WHEN LOWER(name) LIKE '%nottingham%' THEN 'Nottingham Forest'
            WHEN LOWER(name) LIKE '%bournemouth%' THEN 'AFC Bournemouth'
            WHEN LOWER(name) LIKE '%wolves%' OR LOWER(name) LIKE '%wolverhampton%' THEN 'Wolverhampton Wanderers'
            WHEN LOWER(name) LIKE '%ipswich%' THEN 'Ipswich Town'
            ELSE name
        END
    ) as duplicate_count
FROM teams
WHERE 
    LOWER(name) LIKE '%leicester%' OR
    LOWER(name) LIKE '%tottenham%' OR
    LOWER(name) LIKE '%brighton%' OR
    LOWER(name) LIKE '%newcastle%' OR
    LOWER(name) LIKE '%manchester u%' OR
    LOWER(name) LIKE '%west ham%' OR
    LOWER(name) LIKE '%nottingham%' OR
    LOWER(name) LIKE '%bournemouth%' OR
    LOWER(name) LIKE '%wolves%' OR LOWER(name) LIKE '%wolverhampton%' OR
    LOWER(name) LIKE '%ipswich%'
ORDER BY name;

\echo ''
\echo '2️⃣  MERGING DUPLICATE TEAMS...'

-- Leicester City (keep) vs Leicester (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Leicester City' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Leicester');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Leicester City' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Leicester');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Leicester City' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Leicester');
DELETE FROM teams WHERE name = 'Leicester';

-- Tottenham Hotspur (keep) vs Tottenham (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Tottenham Hotspur' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Tottenham');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Tottenham Hotspur' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Tottenham');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Tottenham Hotspur' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Tottenham');
DELETE FROM teams WHERE name = 'Tottenham';

-- Brighton & Hove Albion (keep) vs Brighton (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Brighton & Hove Albion' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Brighton');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Brighton & Hove Albion' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Brighton');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Brighton & Hove Albion' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Brighton');
DELETE FROM teams WHERE name = 'Brighton';

-- Newcastle United (keep) vs Newcastle (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Newcastle United' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Newcastle');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Newcastle United' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Newcastle');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Newcastle United' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Newcastle');
DELETE FROM teams WHERE name = 'Newcastle';

-- Manchester United (keep) vs Manchester Utd (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Manchester United' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Manchester Utd');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Manchester United' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Manchester Utd');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Manchester United' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Manchester Utd');
DELETE FROM teams WHERE name = 'Manchester Utd';

-- West Ham United (keep) vs West Ham (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'West Ham United' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'West Ham');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'West Ham United' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'West Ham');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'West Ham United' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'West Ham');
DELETE FROM teams WHERE name = 'West Ham';

-- Nottingham Forest (keep) vs Nottingham (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Nottingham Forest' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Nottingham');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Nottingham Forest' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Nottingham');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Nottingham Forest' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Nottingham');
DELETE FROM teams WHERE name = 'Nottingham';

-- AFC Bournemouth (keep) vs Bournemouth (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'AFC Bournemouth' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Bournemouth');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'AFC Bournemouth' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Bournemouth');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'AFC Bournemouth' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Bournemouth');
DELETE FROM teams WHERE name = 'Bournemouth';

-- Wolverhampton Wanderers (keep) vs Wolves (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Wolverhampton Wanderers' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Wolves');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Wolverhampton Wanderers' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Wolves');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Wolverhampton Wanderers' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Wolves');
DELETE FROM teams WHERE name = 'Wolves';

-- Ipswich Town (keep) vs Ipswich (merge) - ONLY senior men's team
UPDATE matches SET home_team_id = (SELECT team_id FROM teams WHERE name = 'Ipswich Town' LIMIT 1)
WHERE home_team_id IN (SELECT team_id FROM teams WHERE name = 'Ipswich');
UPDATE matches SET away_team_id = (SELECT team_id FROM teams WHERE name = 'Ipswich Town' LIMIT 1)
WHERE away_team_id IN (SELECT team_id FROM teams WHERE name = 'Ipswich');
UPDATE players SET team_id = (SELECT team_id FROM teams WHERE name = 'Ipswich Town' LIMIT 1)
WHERE team_id IN (SELECT team_id FROM teams WHERE name = 'Ipswich');
DELETE FROM teams WHERE name = 'Ipswich';

\echo ''
\echo '3️⃣  VERIFYING STANDARDIZATION...'
SELECT 
    name,
    COUNT(*) as team_count
FROM teams
WHERE 
    LOWER(name) LIKE '%leicester%' OR
    LOWER(name) LIKE '%tottenham%' OR
    LOWER(name) LIKE '%brighton%' OR
    LOWER(name) LIKE '%newcastle%' OR
    LOWER(name) LIKE '%manchester u%' OR
    LOWER(name) LIKE '%west ham%' OR
    LOWER(name) LIKE '%nottingham%' OR
    LOWER(name) LIKE '%bournemouth%' OR
    LOWER(name) LIKE '%wolves%' OR LOWER(name) LIKE '%wolverhampton%' OR
    LOWER(name) LIKE '%ipswich%'
GROUP BY name
ORDER BY name;

\echo ''
\echo '4️⃣  FINAL TEAM COUNT...'
SELECT COUNT(*) as total_unique_teams FROM teams;

COMMIT;

\echo ''
\echo '================================================================================'
\echo '✅ STANDARDIZATION COMPLETE'
\echo '================================================================================'
\echo ''
