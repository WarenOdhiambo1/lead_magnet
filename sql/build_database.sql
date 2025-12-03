-- =============================================
-- 0. CLEAN SLATE (Delete old structure)
-- =============================================
DROP VIEW IF EXISTS view_scam_detector CASCADE;
DROP VIEW IF EXISTS view_best_prices CASCADE;
DROP TABLE IF EXISTS user_accounts CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS finance_ledger CASCADE;
DROP TABLE IF EXISTS opportunities CASCADE;
DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS market_odds CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS coaches CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS bookmakers CASCADE;
DROP TABLE IF EXISTS referees CASCADE;
DROP TABLE IF EXISTS venues CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;
DROP TABLE IF EXISTS seasons CASCADE;

-- (Your existing code starts below here...)



-- =============================================
-- 1. REFERENCE DATA (Static & Lookup Tables)
-- =============================================

CREATE TABLE seasons (
    season_id SERIAL PRIMARY KEY,
    name VARCHAR(20) UNIQUE,   -- e.g., '2024/2025'
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN DEFAULT FALSE
);

CREATE TABLE leagues (
    league_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,  -- e.g., 'La Liga'
    country VARCHAR(50),
    tier INTEGER,              -- 1 = Top Flight
    current_season_id INTEGER REFERENCES seasons(season_id)
);

CREATE TABLE venues (
    venue_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(100),
    capacity INTEGER,
    altitude_meters INTEGER,   
    surface_type VARCHAR(50)   -- 'Grass', 'Turf'
);

CREATE TABLE referees (
    referee_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    avg_cards_per_game DECIMAL(4,2), 
    penalty_awarded_freq DECIMAL(4,2)
);

CREATE TABLE bookmakers (
    bookie_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    trust_rating INTEGER DEFAULT 5, -- 1-10
    api_endpoint VARCHAR(255),
    commission_percent DECIMAL(4,2) DEFAULT 5.00
);

-- =============================================
-- 2. TEAM & PERSONNEL ASSETS
-- =============================================

CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    league_id INTEGER REFERENCES leagues(league_id),
    venue_id INTEGER REFERENCES venues(venue_id),
    elo_rating DECIMAL(10,2) DEFAULT 1500.00,
    attack_strength DECIMAL(5,2), 
    defense_strength DECIMAL(5,2),
    UNIQUE(name, league_id)
);

CREATE TABLE coaches (
    coach_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    current_team_id INTEGER REFERENCES teams(team_id) ON DELETE SET NULL,
    preferred_formation VARCHAR(20), 
    career_win_pct DECIMAL(5,2),
    trophies_won INTEGER DEFAULT 0
);

CREATE TABLE players (
    player_id BIGSERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id) ON DELETE SET NULL,
    name VARCHAR(100),
    position VARCHAR(10), -- GK, DEF, MID, FWD
    
    -- Medical & Status
    is_injured BOOLEAN DEFAULT FALSE,
    expected_return DATE,
    is_legendary BOOLEAN DEFAULT FALSE,
    
    -- Performance Metrics
    goals_season INTEGER DEFAULT 0,
    assists_season INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    defender_rank_score DECIMAL(5,2),
    
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- 3. MATCH ENGINE
-- =============================================

CREATE TABLE matches (
    match_id BIGSERIAL PRIMARY KEY,
    league_id INTEGER REFERENCES leagues(league_id),
    season_id INTEGER REFERENCES seasons(season_id),
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    venue_id INTEGER REFERENCES venues(venue_id),
    referee_id INTEGER REFERENCES referees(referee_id),
    
    kickoff_time TIMESTAMPTZ NOT NULL,
    
    -- Results
    home_score INTEGER,
    away_score INTEGER,
    status VARCHAR(20) DEFAULT 'SCHEDULED', -- SCHEDULED, LIVE, FINISHED, POSTPONED
    
    CONSTRAINT check_teams CHECK (home_team_id != away_team_id)
);

-- =============================================
-- 4. MARKET DATA (The Massive Table)
-- =============================================

CREATE TABLE market_odds (
    odd_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT REFERENCES matches(match_id) ON DELETE CASCADE,
    bookie_id INTEGER REFERENCES bookmakers(bookie_id),
    
    market_type VARCHAR(50), -- '1x2', 'Correct Score', 'Over/Under'
    selection VARCHAR(100),  -- 'Home', '0:0', 'Under 0.5'
    
    odds DECIMAL(10, 4),     -- High precision for arbitrage (e.g. 1.0526)
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    
    -- Prevent duplicates: A bookie can only have one active price per selection per match
    UNIQUE(match_id, bookie_id, market_type, selection)
);

-- =============================================
-- 5. INTELLIGENCE & FINANCE LAYERS
-- =============================================

CREATE TABLE predictions (
    pred_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT REFERENCES matches(match_id) ON DELETE CASCADE,
    
    -- Probabilities (0.0000 to 1.0000)
    prob_home DECIMAL(6,4), 
    prob_draw DECIMAL(6,4),
    prob_away DECIMAL(6,4),
    
    xg_home DECIMAL(5,2),
    xg_away DECIMAL(5,2),
    
    generated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE opportunities (
    opp_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT REFERENCES matches(match_id),
    type VARCHAR(50), -- 'Arbitrage', 'Value Bet', 'Scam Avoidance'
    
    description TEXT, 
    
    -- Bookie A details
    bookie_a_id INTEGER REFERENCES bookmakers(bookie_id),
    odds_a DECIMAL(10,2),
    
    -- Bookie B details (if applicable)
    bookie_b_id INTEGER REFERENCES bookmakers(bookie_id),
    odds_b DECIMAL(10,2),
    
    profit_margin_percent DECIMAL(5,2),
    status VARCHAR(20) DEFAULT 'OPEN' -- OPEN, EXECUTED, EXPIRED
);

CREATE TABLE finance_ledger (
    transaction_id BIGSERIAL PRIMARY KEY,
    opp_id BIGINT REFERENCES opportunities(opp_id),
    
    stake_amount DECIMAL(15,2),
    odds_taken DECIMAL(10,2),
    potential_return DECIMAL(15,2),
    
    outcome VARCHAR(20) DEFAULT 'PENDING', -- PENDING, WON, LOST, VOID
    actual_profit DECIMAL(15,2) DEFAULT 0.00,
    
    bankroll_balance DECIMAL(15,2),
    bet_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    action_type VARCHAR(50), 
    entity_id BIGINT,        
    entity_type VARCHAR(50), 
    performed_by VARCHAR(100) DEFAULT 'SYSTEM',
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    details JSONB -- Using JSONB is better for flexible logs
);

CREATE TABLE user_accounts (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE,
    password_hash VARCHAR(255),
    role VARCHAR(20) DEFAULT 'ANALYST',
    last_login TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- 6. SMART VIEWS (The Business Logic)
-- =============================================

-- VIEW 1: Ultra-Fast Best Price Finder
-- Uses PostgreSQL 'DISTINCT ON' for max performance
CREATE OR REPLACE VIEW view_best_prices AS
SELECT DISTINCT ON (m.match_id, m.market_type, m.selection)
    m.match_id,
    m.market_type,
    m.selection,
    m.odds as best_price,
    b.name as best_bookmaker,
    m.timestamp as last_updated
FROM market_odds m
JOIN bookmakers b ON m.bookie_id = b.bookie_id
ORDER BY m.match_id, m.market_type, m.selection, m.odds DESC;

-- VIEW 2: The "Zero Goal" Scam Detector
-- Instantly compares the 4 synonyms for 0-0
CREATE OR REPLACE VIEW view_scam_detector AS
SELECT 
    m.match_id,
    'Zero Goal Scenario' as logic_group,
    m.selection,
    m.odds,
    b.name as bookie
FROM market_odds m
JOIN bookmakers b ON m.bookie_id = b.bookie_id
WHERE m.selection IN ('Correct Score 0:0', 'No Goal', 'Under 0.5', '1st Goal None', 'Multigoals No Goal');

-- =============================================
-- 7. OPTIMIZED INDEXES (Crucial for Speed)
-- =============================================

-- Matches: Find by time and status (for the scraper)
CREATE INDEX idx_matches_kickoff ON matches(kickoff_time);
CREATE INDEX idx_matches_status ON matches(status);

-- Odds: The most queried table. Needs composite indexes.
-- This index speeds up "Find me all odds for Match X"
CREATE INDEX idx_market_odds_match ON market_odds(match_id);
-- This index speeds up "Find the best price for Selection Y"
CREATE INDEX idx_market_odds_lookup ON market_odds(match_id, market_type, selection, odds DESC);

-- Players/Teams: For your prediction engine
CREATE INDEX idx_players_team ON players(team_id);
CREATE INDEX idx_teams_league ON teams(league_id);

-- Opportunities: For the Alert System
CREATE INDEX idx_opps_status ON opportunities(status);

-- Finance: For Reporting
CREATE INDEX idx_finance_date ON finance_ledger(bet_date); 