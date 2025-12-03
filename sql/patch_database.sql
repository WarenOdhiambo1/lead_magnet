-- 1. ADD MOTIVATION FACTOR (League Standings)
CREATE TABLE league_standings (
    standing_id SERIAL PRIMARY KEY,
    league_id INTEGER REFERENCES leagues(league_id),
    season_id INTEGER REFERENCES seasons(season_id),
    team_id INTEGER REFERENCES teams(team_id),
    
    position INTEGER,       -- e.g. 1 (Fighting for Title) or 18 (Relegation Battle)
    points INTEGER,
    goal_difference INTEGER,
    form_last_5 VARCHAR(5), -- e.g. 'WWLDW' (Visual trend)
    
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. ADD GOALKEEPER & DETAILED PLAYER STATS
-- We add these columns to the existing 'players' table
ALTER TABLE players 
ADD COLUMN gk_save_pct DECIMAL(5,2) DEFAULT 0.00,
ADD COLUMN matches_started_percent DECIMAL(5,2) DEFAULT 0.00; -- Is he a bench warmer or starter?

-- 3. ADD HEAD-TO-HEAD CACHE
-- Stores the "History with the team it is having now"
CREATE TABLE h2h_trends (
    trend_id SERIAL PRIMARY KEY,
    team_a_id INTEGER REFERENCES teams(team_id),
    team_b_id INTEGER REFERENCES teams(team_id),
    
    matches_played INTEGER,
    team_a_wins INTEGER,
    team_b_wins INTEGER,
    draws INTEGER,
    avg_goals_per_match DECIMAL(4,2),
    
    last_meeting_date DATE,
    winner_last_meeting INTEGER REFERENCES teams(team_id),
    
    UNIQUE(team_a_id, team_b_id)
);

-- 4. ADD BETTING STRATEGY TAGS
-- You asked to "add more profitable strategies"
ALTER TABLE market_odds 
ADD COLUMN strategy_tag VARCHAR(50); -- e.g. 'Value_Bet', 'Hedge_Risk', 'Long_Shot'