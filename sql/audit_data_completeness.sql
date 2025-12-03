-- ============================================================================
-- 🔍 COMPREHENSIVE DATA COMPLETENESS AUDIT
-- ============================================================================
-- Check EVERY column in EVERY table for NULL or empty values
-- ============================================================================

\echo '================================================================================'
\echo '🔍 COMPREHENSIVE DATA COMPLETENESS AUDIT'
\echo '================================================================================'
\echo ''

\echo '1️⃣  TEAMS TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_teams,
    COUNT(team_id) as has_team_id,
    COUNT(name) as has_name,
    COUNT(league_id) as has_league_id,
    COUNT(venue_id) as has_venue_id,
    COUNT(elo_rating) as has_elo_rating,
    COUNT(attack_strength) as has_attack_strength,
    COUNT(defense_strength) as has_defense_strength,
    ROUND(COUNT(name) * 100.0 / COUNT(*), 1) as name_pct,
    ROUND(COUNT(venue_id) * 100.0 / COUNT(*), 1) as venue_pct,
    ROUND(COUNT(attack_strength) * 100.0 / COUNT(*), 1) as attack_pct,
    ROUND(COUNT(defense_strength) * 100.0 / COUNT(*), 1) as defense_pct
FROM teams;

\echo ''
\echo '2️⃣  PLAYERS TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_players,
    COUNT(player_id) as has_player_id,
    COUNT(name) as has_name,
    COUNT(team_id) as has_team_id,
    COUNT(position) as has_position,
    COUNT(CASE WHEN is_injured IS NOT NULL THEN 1 END) as has_injury_status,
    COUNT(CASE WHEN goals_season > 0 THEN 1 END) as has_goals,
    COUNT(CASE WHEN assists_season > 0 THEN 1 END) as has_assists,
    COUNT(CASE WHEN minutes_played > 0 THEN 1 END) as has_minutes,
    ROUND(COUNT(name) * 100.0 / COUNT(*), 1) as name_pct,
    ROUND(COUNT(position) * 100.0 / COUNT(*), 1) as position_pct,
    ROUND(COUNT(CASE WHEN minutes_played > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as minutes_pct
FROM players;

\echo ''
\echo '3️⃣  MATCHES TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_matches,
    COUNT(match_id) as has_match_id,
    COUNT(league_id) as has_league_id,
    COUNT(season_id) as has_season_id,
    COUNT(home_team_id) as has_home_team,
    COUNT(away_team_id) as has_away_team,
    COUNT(venue_id) as has_venue,
    COUNT(referee_id) as has_referee,
    COUNT(kickoff_time) as has_kickoff,
    COUNT(status) as has_status,
    COUNT(home_score) as has_home_score,
    COUNT(away_score) as has_away_score,
    ROUND(COUNT(venue_id) * 100.0 / COUNT(*), 1) as venue_pct,
    ROUND(COUNT(referee_id) * 100.0 / COUNT(*), 1) as referee_pct,
    ROUND(COUNT(home_score) * 100.0 / COUNT(*), 1) as score_pct
FROM matches;

\echo ''
\echo '4️⃣  VENUES TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_venues,
    COUNT(venue_id) as has_venue_id,
    COUNT(name) as has_name,
    COUNT(city) as has_city,
    COUNT(CASE WHEN capacity > 0 THEN 1 END) as has_capacity,
    ROUND(COUNT(name) * 100.0 / NULLIF(COUNT(*), 0), 1) as name_pct,
    ROUND(COUNT(city) * 100.0 / NULLIF(COUNT(*), 0), 1) as city_pct,
    ROUND(COUNT(CASE WHEN capacity > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as capacity_pct
FROM venues;

\echo ''
\echo '5️⃣  REFEREES TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_referees,
    COUNT(referee_id) as has_referee_id,
    COUNT(name) as has_name,
    COUNT(CASE WHEN avg_cards_per_game > 0 THEN 1 END) as has_stats,
    ROUND(COUNT(name) * 100.0 / COUNT(*), 1) as name_pct,
    ROUND(COUNT(CASE WHEN avg_cards_per_game > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as stats_pct
FROM referees;

\echo ''
\echo '6️⃣  MARKET_ODDS TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_odds,
    COUNT(odd_id) as has_odd_id,
    COUNT(match_id) as has_match_id,
    COUNT(bookie_id) as has_bookie_id,
    COUNT(market_type) as has_market_type,
    COUNT(selection) as has_selection,
    COUNT(odds) as has_odds,
    ROUND(COUNT(odds) * 100.0 / COUNT(*), 1) as odds_pct
FROM market_odds;

\echo ''
\echo '7️⃣  LEAGUE_STANDINGS TABLE - Column Completeness:'
SELECT 
    COUNT(*) as total_standings,
    COUNT(standing_id) as has_standing_id,
    COUNT(team_id) as has_team_id,
    COUNT(position) as has_position,
    COUNT(points) as has_points,
    COUNT(goal_difference) as has_gd,
    COUNT(form_last_5) as has_form,
    ROUND(COUNT(position) * 100.0 / NULLIF(COUNT(*), 0), 1) as position_pct,
    ROUND(COUNT(points) * 100.0 / NULLIF(COUNT(*), 0), 1) as points_pct
FROM league_standings;

\echo ''
\echo '================================================================================'
\echo '🔍 DETAILED INCOMPLETE RECORDS CHECK'
\echo '================================================================================'

\echo ''
\echo '❌ TEAMS with NULL values:'
SELECT 
    team_id,
    name,
    CASE WHEN league_id IS NULL THEN 'NO LEAGUE' ELSE '' END as league_issue,
    CASE WHEN venue_id IS NULL THEN 'NO VENUE' ELSE '' END as venue_issue,
    CASE WHEN attack_strength IS NULL THEN 'NO ATTACK' ELSE '' END as attack_issue,
    CASE WHEN defense_strength IS NULL THEN 'NO DEFENSE' ELSE '' END as defense_issue,
    CASE WHEN elo_rating IS NULL THEN 'NO ELO' ELSE '' END as elo_issue
FROM teams
WHERE league_id IS NULL 
   OR attack_strength IS NULL 
   OR defense_strength IS NULL 
   OR elo_rating IS NULL
LIMIT 10;

\echo ''
\echo '❌ PLAYERS with NULL values:'
SELECT 
    player_id,
    name,
    team_id,
    position,
    minutes_played,
    CASE WHEN team_id IS NULL THEN 'NO TEAM' ELSE '' END as team_issue,
    CASE WHEN position IS NULL THEN 'NO POSITION' ELSE '' END as position_issue,
    CASE WHEN minutes_played = 0 THEN 'NO MINUTES' ELSE '' END as minutes_issue
FROM players
WHERE team_id IS NULL 
   OR position IS NULL 
   OR minutes_played = 0
LIMIT 10;

\echo ''
\echo '❌ MATCHES with NULL values:'
SELECT 
    match_id,
    home_team_id,
    away_team_id,
    venue_id,
    referee_id,
    status,
    home_score,
    away_score,
    CASE WHEN venue_id IS NULL THEN 'NO VENUE' ELSE '' END as venue_issue,
    CASE WHEN referee_id IS NULL THEN 'NO REFEREE' ELSE '' END as referee_issue,
    CASE WHEN status = 'FINISHED' AND home_score IS NULL THEN 'NO SCORE' ELSE '' END as score_issue
FROM matches
WHERE venue_id IS NULL 
   OR referee_id IS NULL 
   OR (status = 'FINISHED' AND (home_score IS NULL OR away_score IS NULL))
LIMIT 10;

\echo ''
\echo '================================================================================'
\echo '📊 FINAL DATA QUALITY SCORE'
\echo '================================================================================'

WITH completeness AS (
    SELECT 
        'Teams' as entity,
        COUNT(*) as total,
        COUNT(CASE WHEN name IS NOT NULL 
                    AND league_id IS NOT NULL 
                    AND attack_strength IS NOT NULL 
                    AND defense_strength IS NOT NULL 
                    AND elo_rating IS NOT NULL 
               THEN 1 END) as complete
    FROM teams
    UNION ALL
    SELECT 
        'Players',
        COUNT(*),
        COUNT(CASE WHEN name IS NOT NULL 
                    AND team_id IS NOT NULL 
                    AND position IS NOT NULL 
               THEN 1 END)
    FROM players
    UNION ALL
    SELECT 
        'Matches',
        COUNT(*),
        COUNT(CASE WHEN home_team_id IS NOT NULL 
                    AND away_team_id IS NOT NULL 
                    AND kickoff_time IS NOT NULL 
                    AND status IS NOT NULL 
               THEN 1 END)
    FROM matches
    UNION ALL
    SELECT 
        'Referees',
        COUNT(*),
        COUNT(CASE WHEN name IS NOT NULL 
                    AND avg_cards_per_game IS NOT NULL 
               THEN 1 END)
    FROM referees
)
SELECT 
    entity,
    total,
    complete,
    ROUND(complete * 100.0 / NULLIF(total, 0), 1) as completeness_pct,
    CASE 
        WHEN complete * 100.0 / NULLIF(total, 0) >= 95 THEN '✅ EXCELLENT'
        WHEN complete * 100.0 / NULLIF(total, 0) >= 80 THEN '⚠️  GOOD'
        ELSE '❌ POOR'
    END as quality_grade
FROM completeness
ORDER BY completeness_pct DESC;

\echo ''
\echo '================================================================================'
\echo '✅ AUDIT COMPLETE'
\echo '================================================================================'
\echo ''
