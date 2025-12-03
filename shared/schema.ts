import { pgTable, text, integer, bigint, numeric, timestamp, boolean, date, jsonb, serial, bigserial } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Core Tables

export const seasons = pgTable("seasons", {
  seasonId: serial("season_id").primaryKey(),
  name: text("name").notNull(),
  startDate: date("start_date"),
  endDate: date("end_date"),
  isActive: boolean("is_active").default(false),
});

export const leagues = pgTable("leagues", {
  leagueId: serial("league_id").primaryKey(),
  name: text("name").notNull(),
  country: text("country"),
  tier: integer("tier"),
  currentSeasonId: integer("current_season_id").references(() => seasons.seasonId),
});

export const venues = pgTable("venues", {
  venueId: serial("venue_id").primaryKey(),
  name: text("name").notNull(),
  city: text("city"),
  capacity: integer("capacity"),
  altitudeMeters: integer("altitude_meters"),
  surfaceType: text("surface_type"),
});

export const referees = pgTable("referees", {
  refereeId: serial("referee_id").primaryKey(),
  name: text("name").notNull(),
  avgCardsPerGame: numeric("avg_cards_per_game", { precision: 5, scale: 2 }),
  penaltyAwardedFreq: numeric("penalty_awarded_freq", { precision: 5, scale: 3 }),
});

export const teams = pgTable("teams", {
  teamId: serial("team_id").primaryKey(),
  name: text("name").notNull(),
  leagueId: integer("league_id").references(() => leagues.leagueId),
  venueId: integer("venue_id").references(() => venues.venueId),
  eloRating: numeric("elo_rating", { precision: 7, scale: 2 }),
  attackStrength: numeric("attack_strength", { precision: 5, scale: 3 }),
  defenseStrength: numeric("defense_strength", { precision: 5, scale: 3 }),
});

export const coaches = pgTable("coaches", {
  coachId: serial("coach_id").primaryKey(),
  name: text("name").notNull(),
  currentTeamId: integer("current_team_id").references(() => teams.teamId),
  preferredFormation: text("preferred_formation"),
  careerWinPct: numeric("career_win_pct", { precision: 5, scale: 2 }),
  trophiesWon: integer("trophies_won").default(0),
});

export const coachHistory = pgTable("coach_history", {
  historyId: serial("history_id").primaryKey(),
  coachId: integer("coach_id").references(() => coaches.coachId),
  teamId: integer("team_id").references(() => teams.teamId),
  startDate: date("start_date"),
  endDate: date("end_date"),
  matchesManaged: integer("matches_managed"),
  winPercentage: numeric("win_percentage", { precision: 5, scale: 2 }),
});

export const players = pgTable("players", {
  playerId: bigserial("player_id", { mode: "number" }).primaryKey(),
  teamId: integer("team_id").references(() => teams.teamId),
  name: text("name").notNull(),
  position: text("position"),
  isInjured: boolean("is_injured").default(false),
  expectedReturn: date("expected_return"),
  isLegendary: boolean("is_legendary").default(false),
  goalsSeason: integer("goals_season").default(0),
  assistsSeason: integer("assists_season").default(0),
  minutesPlayed: integer("minutes_played").default(0),
  defenderRankScore: numeric("defender_rank_score", { precision: 5, scale: 2 }),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  gkSavePct: numeric("gk_save_pct", { precision: 5, scale: 2 }),
  matchesStartedPercent: numeric("matches_started_percent", { precision: 5, scale: 2 }),
});

export const injuries = pgTable("injuries", {
  injuryId: serial("injury_id").primaryKey(),
  playerId: integer("player_id").references(() => players.playerId),
  injuryType: text("injury_type"),
  startDate: date("start_date"),
  expectedReturnDate: date("expected_return_date"),
  gamesMissed: integer("games_missed").default(0),
});

export const playerPerformanceHistory = pgTable("player_performance_history", {
  statId: bigserial("stat_id", { mode: "number" }).primaryKey(),
  playerId: integer("player_id").references(() => players.playerId),
  seasonId: integer("season_id").references(() => seasons.seasonId),
  teamId: integer("team_id").references(() => teams.teamId),
  minutesPlayed: integer("minutes_played").default(0),
  averageRating: numeric("average_rating", { precision: 4, scale: 2 }),
  goals: integer("goals").default(0),
  assists: integer("assists").default(0),
  xgGenerated: numeric("xg_generated", { precision: 5, scale: 2 }),
  conversionRate: numeric("conversion_rate", { precision: 5, scale: 2 }),
  tacklesWonPct: numeric("tackles_won_pct", { precision: 5, scale: 2 }),
  aerialDuelsWonPct: numeric("aerial_duels_won_pct", { precision: 5, scale: 2 }),
  errorsLeadingToGoal: integer("errors_leading_to_goal").default(0),
  cleanSheets: integer("clean_sheets").default(0),
});

export const leagueStandings = pgTable("league_standings", {
  standingId: serial("standing_id").primaryKey(),
  leagueId: integer("league_id").references(() => leagues.leagueId),
  seasonId: integer("season_id").references(() => seasons.seasonId),
  teamId: integer("team_id").references(() => teams.teamId),
  position: integer("position"),
  points: integer("points").default(0),
  goalDifference: integer("goal_difference").default(0),
  formLast5: text("form_last_5"),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
});

export const matches = pgTable("matches", {
  matchId: bigserial("match_id", { mode: "number" }).primaryKey(),
  leagueId: integer("league_id").references(() => leagues.leagueId),
  seasonId: integer("season_id").references(() => seasons.seasonId),
  homeTeamId: integer("home_team_id").references(() => teams.teamId),
  awayTeamId: integer("away_team_id").references(() => teams.teamId),
  venueId: integer("venue_id").references(() => venues.venueId),
  refereeId: integer("referee_id").references(() => referees.refereeId),
  kickoffTime: timestamp("kickoff_time", { withTimezone: true }).notNull(),
  homeScore: integer("home_score"),
  awayScore: integer("away_score"),
  status: text("status").default("Scheduled"),
});

export const h2hTrends = pgTable("h2h_trends", {
  trendId: serial("trend_id").primaryKey(),
  teamAId: integer("team_a_id").references(() => teams.teamId),
  teamBId: integer("team_b_id").references(() => teams.teamId),
  matchesPlayed: integer("matches_played").default(0),
  teamAWins: integer("team_a_wins").default(0),
  teamBWins: integer("team_b_wins").default(0),
  draws: integer("draws").default(0),
  avgGoalsPerMatch: numeric("avg_goals_per_match", { precision: 4, scale: 2 }),
  lastMeetingDate: date("last_meeting_date"),
  winnerLastMeeting: integer("winner_last_meeting"),
});

export const bookmakers = pgTable("bookmakers", {
  bookieId: serial("bookie_id").primaryKey(),
  name: text("name").notNull(),
  trustRating: integer("trust_rating"),
  apiEndpoint: text("api_endpoint"),
  commissionPercent: numeric("commission_percent", { precision: 5, scale: 2 }),
});

export const marketOdds = pgTable("market_odds", {
  oddId: bigserial("odd_id", { mode: "number" }).primaryKey(),
  matchId: bigint("match_id", { mode: "number" }).references(() => matches.matchId),
  bookieId: integer("bookie_id").references(() => bookmakers.bookieId),
  marketType: text("market_type"),
  selection: text("selection"),
  odds: numeric("odds", { precision: 7, scale: 3 }),
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
  strategyTag: text("strategy_tag"),
});

export const predictions = pgTable("predictions", {
  predId: bigserial("pred_id", { mode: "number" }).primaryKey(),
  matchId: bigint("match_id", { mode: "number" }).references(() => matches.matchId),
  probHome: numeric("prob_home", { precision: 5, scale: 4 }),
  probDraw: numeric("prob_draw", { precision: 5, scale: 4 }),
  probAway: numeric("prob_away", { precision: 5, scale: 4 }),
  xgHome: numeric("xg_home", { precision: 4, scale: 2 }),
  xgAway: numeric("xg_away", { precision: 4, scale: 2 }),
  generatedAt: timestamp("generated_at", { withTimezone: true }).defaultNow(),
});

export const opportunities = pgTable("opportunities", {
  oppId: bigserial("opp_id", { mode: "number" }).primaryKey(),
  matchId: bigint("match_id", { mode: "number" }).references(() => matches.matchId),
  type: text("type"),
  description: text("description"),
  bookieAId: integer("bookie_a_id").references(() => bookmakers.bookieId),
  oddsA: numeric("odds_a", { precision: 7, scale: 3 }),
  bookieBId: integer("bookie_b_id").references(() => bookmakers.bookieId),
  oddsB: numeric("odds_b", { precision: 7, scale: 3 }),
  profitMarginPercent: numeric("profit_margin_percent", { precision: 6, scale: 3 }),
  status: text("status").default("Active"),
});

export const calculatedOpportunities = pgTable("calculated_opportunities", {
  oppId: bigserial("opp_id", { mode: "number" }).primaryKey(),
  matchId: integer("match_id").references(() => matches.matchId),
  marketType: text("market_type"),
  bestHomePrice: numeric("best_home_price", { precision: 7, scale: 3 }),
  bestDrawPrice: numeric("best_draw_price", { precision: 7, scale: 3 }),
  bestAwayPrice: numeric("best_away_price", { precision: 7, scale: 3 }),
  totalMarketPercentage: numeric("total_market_percentage", { precision: 6, scale: 3 }),
  guaranteedProfitMargin: numeric("guaranteed_profit_margin", { precision: 6, scale: 3 }),
  detectedAt: timestamp("detected_at").defaultNow(),
});

export const financeLedger = pgTable("finance_ledger", {
  transactionId: bigserial("transaction_id", { mode: "number" }).primaryKey(),
  oppId: bigint("opp_id", { mode: "number" }).references(() => opportunities.oppId),
  stakeAmount: numeric("stake_amount", { precision: 10, scale: 2 }),
  oddsTaken: numeric("odds_taken", { precision: 7, scale: 3 }),
  potentialReturn: numeric("potential_return", { precision: 10, scale: 2 }),
  outcome: text("outcome"),
  actualProfit: numeric("actual_profit", { precision: 10, scale: 2 }),
  bankrollBalance: numeric("bankroll_balance", { precision: 10, scale: 2 }),
  betDate: timestamp("bet_date", { withTimezone: true }).defaultNow(),
});

export const userAccounts = pgTable("user_accounts", {
  userId: serial("user_id").primaryKey(),
  username: text("username").notNull().unique(),
  passwordHash: text("password_hash").notNull(),
  role: text("role").default("trader"),
  lastLogin: timestamp("last_login", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

export const auditLog = pgTable("audit_log", {
  logId: bigserial("log_id", { mode: "number" }).primaryKey(),
  actionType: text("action_type"),
  entityId: bigint("entity_id", { mode: "number" }),
  entityType: text("entity_type"),
  performedBy: text("performed_by"),
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
  details: jsonb("details"),
});

// Insert Schemas
export const insertSeasonSchema = createInsertSchema(seasons).omit({ seasonId: true });
export const insertLeagueSchema = createInsertSchema(leagues).omit({ leagueId: true });
export const insertVenueSchema = createInsertSchema(venues).omit({ venueId: true });
export const insertRefereeSchema = createInsertSchema(referees).omit({ refereeId: true });
export const insertTeamSchema = createInsertSchema(teams).omit({ teamId: true });
export const insertCoachSchema = createInsertSchema(coaches).omit({ coachId: true });
export const insertPlayerSchema = createInsertSchema(players).omit({ playerId: true, updatedAt: true });
export const insertMatchSchema = createInsertSchema(matches).omit({ matchId: true });
export const insertBookmakerSchema = createInsertSchema(bookmakers).omit({ bookieId: true });
export const insertMarketOddsSchema = createInsertSchema(marketOdds).omit({ oddId: true, timestamp: true });
export const insertPredictionSchema = createInsertSchema(predictions).omit({ predId: true, generatedAt: true });
export const insertOpportunitySchema = createInsertSchema(opportunities).omit({ oppId: true });

// Types
export type Season = typeof seasons.$inferSelect;
export type League = typeof leagues.$inferSelect;
export type Venue = typeof venues.$inferSelect;
export type Referee = typeof referees.$inferSelect;
export type Team = typeof teams.$inferSelect;
export type Coach = typeof coaches.$inferSelect;
export type Player = typeof players.$inferSelect;
export type Match = typeof matches.$inferSelect;
export type Bookmaker = typeof bookmakers.$inferSelect;
export type MarketOdds = typeof marketOdds.$inferSelect;
export type Prediction = typeof predictions.$inferSelect;
export type Opportunity = typeof opportunities.$inferSelect;
export type CalculatedOpportunity = typeof calculatedOpportunities.$inferSelect;
export type FinanceLedger = typeof financeLedger.$inferSelect;

export type InsertSeason = z.infer<typeof insertSeasonSchema>;
export type InsertLeague = z.infer<typeof insertLeagueSchema>;
export type InsertVenue = z.infer<typeof insertVenueSchema>;
export type InsertReferee = z.infer<typeof insertRefereeSchema>;
export type InsertTeam = z.infer<typeof insertTeamSchema>;
export type InsertCoach = z.infer<typeof insertCoachSchema>;
export type InsertPlayer = z.infer<typeof insertPlayerSchema>;
export type InsertMatch = z.infer<typeof insertMatchSchema>;
export type InsertBookmaker = z.infer<typeof insertBookmakerSchema>;
export type InsertMarketOdds = z.infer<typeof insertMarketOddsSchema>;
export type InsertPrediction = z.infer<typeof insertPredictionSchema>;
export type InsertOpportunity = z.infer<typeof insertOpportunitySchema>;
