import { db } from "./db";
import { 
  type Match, type InsertMatch,
  type Team, type InsertTeam,
  type League, type InsertLeague,
  type Season, type InsertSeason,
  type Venue, type InsertVenue,
  type Referee, type InsertReferee,
  type Bookmaker, type InsertBookmaker,
  type MarketOdds, type InsertMarketOdds,
  type Prediction, type InsertPrediction,
  type Opportunity, type InsertOpportunity,
  matches, teams, leagues, seasons, venues, referees, bookmakers, marketOdds, predictions, opportunities
} from "@shared/schema";
import { eq, and, gte, desc, sql } from "drizzle-orm";

export interface IStorage {
  // Matches
  getMatches(limit?: number): Promise<Match[]>;
  getMatchById(id: number): Promise<Match | undefined>;
  getUpcomingMatches(): Promise<Match[]>;
  createMatch(match: InsertMatch): Promise<Match>;
  updateMatchScore(id: number, homeScore: number, awayScore: number, status: string): Promise<void>;
  
  // Teams
  getTeams(): Promise<Team[]>;
  getTeamById(id: number): Promise<Team | undefined>;
  getTeamsByLeague(leagueId: number): Promise<Team[]>;
  createTeam(team: InsertTeam): Promise<Team>;
  
  // Leagues
  getLeagues(): Promise<League[]>;
  createLeague(league: InsertLeague): Promise<League>;
  
  // Seasons
  getActiveSeasons(): Promise<Season[]>;
  createSeason(season: InsertSeason): Promise<Season>;
  
  // Bookmakers
  getBookmakers(): Promise<Bookmaker[]>;
  createBookmaker(bookmaker: InsertBookmaker): Promise<Bookmaker>;
  
  // Market Odds
  getOddsForMatch(matchId: number): Promise<MarketOdds[]>;
  createMarketOdds(odds: InsertMarketOdds): Promise<MarketOdds>;
  
  // Predictions
  getPredictionForMatch(matchId: number): Promise<Prediction | undefined>;
  createPrediction(prediction: InsertPrediction): Promise<Prediction>;
  
  // Opportunities
  getActiveOpportunities(limit?: number): Promise<Array<Opportunity & { match: Match; team_home: Team; team_away: Team }>>;
  createOpportunity(opportunity: InsertOpportunity): Promise<Opportunity>;
}

export class DatabaseStorage implements IStorage {
  // Matches
  async getMatches(limit: number = 50): Promise<Match[]> {
    return await db.select().from(matches).orderBy(desc(matches.kickoffTime)).limit(limit);
  }

  async getMatchById(id: number): Promise<Match | undefined> {
    const result = await db.select().from(matches).where(eq(matches.matchId, id)).limit(1);
    return result[0];
  }

  async getUpcomingMatches(): Promise<Match[]> {
    const now = new Date();
    return await db.select()
      .from(matches)
      .where(gte(matches.kickoffTime, now))
      .orderBy(matches.kickoffTime)
      .limit(20);
  }

  async createMatch(match: InsertMatch): Promise<Match> {
    const result = await db.insert(matches).values(match).returning();
    return result[0];
  }

  async updateMatchScore(id: number, homeScore: number, awayScore: number, status: string): Promise<void> {
    await db.update(matches)
      .set({ homeScore, awayScore, status })
      .where(eq(matches.matchId, id));
  }

  // Teams
  async getTeams(): Promise<Team[]> {
    return await db.select().from(teams);
  }

  async getTeamById(id: number): Promise<Team | undefined> {
    const result = await db.select().from(teams).where(eq(teams.teamId, id)).limit(1);
    return result[0];
  }

  async getTeamsByLeague(leagueId: number): Promise<Team[]> {
    return await db.select().from(teams).where(eq(teams.leagueId, leagueId));
  }

  async createTeam(team: InsertTeam): Promise<Team> {
    const result = await db.insert(teams).values(team).returning();
    return result[0];
  }

  // Leagues
  async getLeagues(): Promise<League[]> {
    return await db.select().from(leagues);
  }

  async createLeague(league: InsertLeague): Promise<League> {
    const result = await db.insert(leagues).values(league).returning();
    return result[0];
  }

  // Seasons
  async getActiveSeasons(): Promise<Season[]> {
    return await db.select().from(seasons).where(eq(seasons.isActive, true));
  }

  async createSeason(season: InsertSeason): Promise<Season> {
    const result = await db.insert(seasons).values(season).returning();
    return result[0];
  }

  // Bookmakers
  async getBookmakers(): Promise<Bookmaker[]> {
    return await db.select().from(bookmakers);
  }

  async createBookmaker(bookmaker: InsertBookmaker): Promise<Bookmaker> {
    const result = await db.insert(bookmakers).values(bookmaker).returning();
    return result[0];
  }

  // Market Odds
  async getOddsForMatch(matchId: number): Promise<MarketOdds[]> {
    return await db.select().from(marketOdds).where(eq(marketOdds.matchId, matchId));
  }

  async createMarketOdds(odds: InsertMarketOdds): Promise<MarketOdds> {
    const result = await db.insert(marketOdds).values(odds).returning();
    return result[0];
  }

  // Predictions
  async getPredictionForMatch(matchId: number): Promise<Prediction | undefined> {
    const result = await db.select().from(predictions).where(eq(predictions.matchId, matchId)).limit(1);
    return result[0];
  }

  async createPrediction(prediction: InsertPrediction): Promise<Prediction> {
    const result = await db.insert(predictions).values(prediction).returning();
    return result[0];
  }

  // Opportunities
  async getActiveOpportunities(limit: number = 20): Promise<Array<Opportunity & { match: Match; team_home: Team; team_away: Team }>> {
    const result = await db.select({
      opportunity: opportunities,
      match: matches,
      team_home: sql<Team>`(SELECT * FROM ${teams} WHERE ${teams.teamId} = ${matches.homeTeamId})`.as('team_home'),
      team_away: sql<Team>`(SELECT * FROM ${teams} WHERE ${teams.teamId} = ${matches.awayTeamId})`.as('team_away'),
    })
      .from(opportunities)
      .innerJoin(matches, eq(opportunities.matchId, matches.matchId))
      .where(eq(opportunities.status, 'Active'))
      .orderBy(desc(opportunities.oppId))
      .limit(limit);

    return result.map((r: any) => ({ ...r.opportunity, match: r.match, team_home: r.team_home, team_away: r.team_away }));
  }

  async createOpportunity(opportunity: InsertOpportunity): Promise<Opportunity> {
    const result = await db.insert(opportunities).values(opportunity).returning();
    return result[0];
  }
}

export const storage = new DatabaseStorage();
