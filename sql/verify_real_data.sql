-- ============================================================================
-- 🔍 VERIFY REAL DATA - Show recognizable players, teams, matches
-- ============================================================================

\echo '================================================================================'
\echo '🔍 DATA VERIFICATION - Are these REAL players/teams?'
\echo '================================================================================'
\echo ''

\echo '1️⃣  TOP 20 PREMIER LEAGUE TEAMS (You should recognize these):'
SELECT 
    t.name as team_name,
    t.attack_strength,
    t.defense_strength,
    t.elo_rating,
    COUNT(DISTINCT p.player_id) as player_count,
    COUNT(DISTINCT m.match_id) as matches_played
FROM teams t
LEFT JOIN players p ON t.team_id = p.team_id
LEFT JOIN matches m ON t.team_id = m.home_team_id OR t.team_id = m.away_team_id
WHERE t.name NOT LIKE '%U18%' 
  AND t.name NOT LIKE '%U21%' 
  AND t.name NOT LIKE '%U19%'
  AND t.name NOT LIKE '% W'
  AND t.league_id = 2
GROUP BY t.team_id, t.name, t.attack_strength, t.defense_strength, t.elo_rating
ORDER BY matches_played DESC
LIMIT 20;

\echo ''
\echo '2️⃣  SAMPLE PLAYERS FROM FAMOUS TEAMS (You should recognize these names):'

\echo ''
\echo '   LIVERPOOL PLAYERS:'
SELECT 
    p.name,
    p.position,
    p.goals_season,
    p.assists_season,
    p.minutes_played
FROM players p
JOIN teams t ON p.team_id = t.team_id
WHERE t.name = 'Liverpool'
ORDER BY p.minutes_played DESC
LIMIT 10;

\echo ''
\echo '   MANCHESTER CITY PLAYERS:'
SELECT 
    p.name,
    p.position,
    p.goals_season,
    p.assists_season,
    p.minutes_played
FROM players p
JOIN teams t ON p.team_id = t.team_id
WHERE t.name = 'Manchester City'
ORDER BY p.minutes_played DESC
LIMIT 10;

\echo ''
\echo '   ARSENAL PLAYERS:'
SELECT 
    p.name,
    p.position,
    p.goals_season,
    p.assists_season,
    p.minutes_played
FROM players p
JOIN teams t ON p.team_id = t.team_id
WHERE t.name = 'Arsenal'
ORDER BY p.minutes_played DESC
LIMIT 10;

\echo ''
\echo '   CHELSEA PLAYERS:'
SELECT 
    p.name,
    p.position,
    p.goals_season,
    p.assists_season,
    p.minutes_played
FROM players p
JOIN teams t ON p.team_id = t.team_id
WHERE t.name = 'Chelsea'
ORDER BY p.minutes_played DESC
LIMIT 10;

\echo ''
\echo '3️⃣  RECENT REAL MATCHES (You should recognize these fixtures):'
SELECT 
    m.match_id,
    ht.name as home_team,
    at.name as away_team,
    m.home_score,
    m.away_score,
    m.kickoff_time::date as match_date,
    m.status
FROM matches m
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
WHERE m.status = 'FINISHED'
  AND ht.name NOT LIKE '%U18%' 
  AND ht.name NOT LIKE '%U21%'
  AND at.name NOT LIKE '%U18%' 
  AND at.name NOT LIKE '%U21%'
ORDER BY m.kickoff_time DESC
LIMIT 20;

\echo ''
\echo '4️⃣  UPCOMING MATCHES WITH ODDS (Real betting opportunities):'
SELECT 
    m.match_id,
    ht.name as home_team,
    at.name as away_team,
    m.kickoff_time,
    COUNT(DISTINCT mo.bookie_id) as bookmaker_count,
    COUNT(mo.odd_id) as total_odds
FROM matches m
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
LEFT JOIN market_odds mo ON m.match_id = mo.match_id
WHERE m.status = 'SCHEDULED'
  AND m.kickoff_time > NOW()
  AND ht.name NOT LIKE '%U18%' 
  AND ht.name NOT LIKE '%U21%'
  AND at.name NOT LIKE '%U18%' 
  AND at.name NOT LIKE '%U21%'
GROUP BY m.match_id, ht.name, at.name, m.kickoff_time
HAVING COUNT(mo.odd_id) > 0
ORDER BY m.kickoff_time
LIMIT 15;

\echo ''
\echo '5️⃣  TOP GOALSCORERS (You should recognize these names):'
SELECT 
    p.name,
    t.name as team,
    p.position,
    p.goals_season,
    p.assists_season,
    p.minutes_played
FROM players p
JOIN teams t ON p.team_id = t.team_id
WHERE p.goals_season > 0
  AND t.name NOT LIKE '%U18%' 
  AND t.name NOT LIKE '%U21%'
  AND t.name NOT LIKE '% W'
ORDER BY p.goals_season DESC
LIMIT 20;

\echo ''
\echo '6️⃣  SAMPLE BOOKMAKER ODDS (Real betting data):'
SELECT 
    b.name as bookmaker,
    ht.name as home_team,
    at.name as away_team,
    mo.market_type,
    mo.selection,
    mo.odds
FROM market_odds mo
JOIN bookmakers b ON mo.bookie_id = b.bookie_id
JOIN matches m ON mo.match_id = m.match_id
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
WHERE mo.market_type = 'h2h'
  AND m.status = 'SCHEDULED'
ORDER BY m.kickoff_time, b.name
LIMIT 30;

\echo ''
\echo '================================================================================'
\echo '✅ VERIFICATION COMPLETE'
\echo '================================================================================'
\echo ''
\echo '💡 If you recognize these players/teams/matches, the data is REAL!'
\echo ''
