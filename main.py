from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Sports Quant Engine API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "student_finance_dream"),
        user=os.getenv("DB_USER", "Waren_Dev"),
        password=os.getenv("DB_PASSWORD", ""),
        cursor_factory=RealDictCursor
    )

@app.get("/")
def root():
    return {
        "name": "Sports Quant Engine API",
        "version": "1.0",
        "status": "operational",
        "docs": "/docs"
    }

@app.get("/api/health")
def health_check():
    try:
        conn = get_db()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.get("/api/matches")
def get_matches(limit: int = 50):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.*, 
                       ht.name as home_team_name,
                       at.name as away_team_name,
                       l.name as league_name
                FROM matches m
                JOIN teams ht ON m.home_team_id = ht.team_id
                JOIN teams at ON m.away_team_id = at.team_id
                JOIN leagues l ON m.league_id = l.league_id
                ORDER BY m.kickoff_time DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()

@app.get("/api/matches/upcoming")
def get_upcoming_matches():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.*, 
                       ht.name as home_team_name,
                       at.name as away_team_name,
                       l.name as league_name
                FROM matches m
                JOIN teams ht ON m.home_team_id = ht.team_id
                JOIN teams at ON m.away_team_id = at.team_id
                JOIN leagues l ON m.league_id = l.league_id
                WHERE m.kickoff_time >= NOW()
                ORDER BY m.kickoff_time
                LIMIT 20
            """)
            return cur.fetchall()

@app.get("/api/opportunities")
def get_opportunities(limit: int = 20):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    o.*,
                    m.kickoff_time,
                    ht.name as home_team,
                    at.name as away_team,
                    p.prob_home, p.prob_draw, p.prob_away
                FROM opportunities o
                JOIN matches m ON o.match_id = m.match_id
                JOIN teams ht ON m.home_team_id = ht.team_id
                JOIN teams at ON m.away_team_id = at.team_id
                LEFT JOIN predictions p ON m.match_id = p.match_id
                WHERE o.status = 'OPEN'
                ORDER BY o.profit_margin_percent DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()

@app.get("/api/stats")
def get_stats():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM teams) as teams,
                    (SELECT COUNT(*) FROM players) as players,
                    (SELECT COUNT(*) FROM matches) as matches,
                    (SELECT COUNT(*) FROM matches WHERE status = 'FINISHED') as finished_matches,
                    (SELECT COUNT(*) FROM matches WHERE kickoff_time >= NOW()) as upcoming_matches,
                    (SELECT COUNT(*) FROM market_odds) as odds,
                    (SELECT COUNT(*) FROM predictions) as predictions,
                    (SELECT COUNT(DISTINCT bookie_id) FROM bookmakers) as bookmakers
            """)
            return cur.fetchone()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
