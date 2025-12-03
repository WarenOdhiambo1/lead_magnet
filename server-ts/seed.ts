import { db } from "./db";
import { 
  seasons, leagues, venues, referees, teams, bookmakers, matches, predictions, opportunities, marketOdds
} from "@shared/schema";

async function seed() {
  console.log("🌱 Seeding database...");

  // Create Season
  const [currentSeason] = await db.insert(seasons).values({
    name: "2024/2025",
    startDate: "2024-08-01",
    endDate: "2025-05-31",
    isActive: true,
  }).returning();

  console.log("✅ Season created");

  // Create Leagues
  const leagueData = [
    { name: "Premier League", country: "England", tier: 1, currentSeasonId: currentSeason.seasonId },
    { name: "La Liga", country: "Spain", tier: 1, currentSeasonId: currentSeason.seasonId },
    { name: "Bundesliga", country: "Germany", tier: 1, currentSeasonId: currentSeason.seasonId },
    { name: "Serie A", country: "Italy", tier: 1, currentSeasonId: currentSeason.seasonId },
    { name: "Ligue 1", country: "France", tier: 1, currentSeasonId: currentSeason.seasonId },
    { name: "Champions League", country: "Europe", tier: 1, currentSeasonId: currentSeason.seasonId },
  ];

  const createdLeagues = await db.insert(leagues).values(leagueData).returning();
  console.log("✅ Leagues created");

  // Create Venues
  const venueData = [
    { name: "Old Trafford", city: "Manchester", capacity: 74879, surfaceType: "Grass" },
    { name: "Anfield", city: "Liverpool", capacity: 54074, surfaceType: "Grass" },
    { name: "Emirates Stadium", city: "London", capacity: 60704, surfaceType: "Grass" },
    { name: "Etihad Stadium", city: "Manchester", capacity: 55097, surfaceType: "Grass" },
    { name: "Santiago Bernabéu", city: "Madrid", capacity: 81044, surfaceType: "Grass" },
    { name: "Camp Nou", city: "Barcelona", capacity: 99354, surfaceType: "Grass" },
    { name: "Allianz Arena", city: "Munich", capacity: 75024, surfaceType: "Grass" },
    { name: "San Siro", city: "Milan", capacity: 75923, surfaceType: "Grass" },
  ];

  const createdVenues = await db.insert(venues).values(venueData).returning();
  console.log("✅ Venues created");

  // Create Referees
  const refereeData = [
    { name: "Michael Oliver", avgCardsPerGame: "4.2", penaltyAwardedFreq: "0.15" },
    { name: "Anthony Taylor", avgCardsPerGame: "3.8", penaltyAwardedFreq: "0.12" },
    { name: "Mateu Lahoz", avgCardsPerGame: "5.1", penaltyAwardedFreq: "0.18" },
    { name: "Felix Brych", avgCardsPerGame: "3.5", penaltyAwardedFreq: "0.10" },
  ];

  const createdReferees = await db.insert(referees).values(refereeData).returning();
  console.log("✅ Referees created");

  // Create Teams for Premier League
  const plTeams = [
    { name: "Arsenal", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[2].venueId, eloRating: "1850", attackStrength: "1.42", defenseStrength: "0.78" },
    { name: "Man City", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[3].venueId, eloRating: "1920", attackStrength: "1.65", defenseStrength: "0.65" },
    { name: "Liverpool", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[1].venueId, eloRating: "1880", attackStrength: "1.58", defenseStrength: "0.72" },
    { name: "Man Utd", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[0].venueId, eloRating: "1780", attackStrength: "1.25", defenseStrength: "0.95" },
    { name: "Chelsea", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[2].venueId, eloRating: "1790", attackStrength: "1.30", defenseStrength: "0.88" },
    { name: "Tottenham", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[2].venueId, eloRating: "1770", attackStrength: "1.35", defenseStrength: "0.92" },
    { name: "Newcastle", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[2].venueId, eloRating: "1750", attackStrength: "1.22", defenseStrength: "0.85" },
    { name: "Aston Villa", leagueId: createdLeagues[0].leagueId, venueId: createdVenues[2].venueId, eloRating: "1720", attackStrength: "1.18", defenseStrength: "0.90" },
  ];

  // La Liga teams
  const laLigaTeams = [
    { name: "Real Madrid", leagueId: createdLeagues[1].leagueId, venueId: createdVenues[4].venueId, eloRating: "1930", attackStrength: "1.68", defenseStrength: "0.68" },
    { name: "Barcelona", leagueId: createdLeagues[1].leagueId, venueId: createdVenues[5].venueId, eloRating: "1900", attackStrength: "1.62", defenseStrength: "0.72" },
    { name: "Atletico Madrid", leagueId: createdLeagues[1].leagueId, venueId: createdVenues[4].venueId, eloRating: "1840", attackStrength: "1.35", defenseStrength: "0.65" },
    { name: "Real Sociedad", leagueId: createdLeagues[1].leagueId, venueId: createdVenues[4].venueId, eloRating: "1740", attackStrength: "1.20", defenseStrength: "0.85" },
  ];

  // Bundesliga teams
  const bundesligaTeams = [
    { name: "Bayern Munich", leagueId: createdLeagues[2].leagueId, venueId: createdVenues[6].venueId, eloRating: "1910", attackStrength: "1.70", defenseStrength: "0.70" },
    { name: "Dortmund", leagueId: createdLeagues[2].leagueId, venueId: createdVenues[6].venueId, eloRating: "1820", attackStrength: "1.48", defenseStrength: "0.82" },
    { name: "RB Leipzig", leagueId: createdLeagues[2].leagueId, venueId: createdVenues[6].venueId, eloRating: "1790", attackStrength: "1.38", defenseStrength: "0.78" },
    { name: "Leverkusen", leagueId: createdLeagues[2].leagueId, venueId: createdVenues[6].venueId, eloRating: "1810", attackStrength: "1.45", defenseStrength: "0.75" },
  ];

  // Serie A teams
  const serieATeams = [
    { name: "Inter", leagueId: createdLeagues[3].leagueId, venueId: createdVenues[7].venueId, eloRating: "1870", attackStrength: "1.52", defenseStrength: "0.68" },
    { name: "Milan", leagueId: createdLeagues[3].leagueId, venueId: createdVenues[7].venueId, eloRating: "1830", attackStrength: "1.42", defenseStrength: "0.75" },
    { name: "Juventus", leagueId: createdLeagues[3].leagueId, venueId: createdVenues[7].venueId, eloRating: "1840", attackStrength: "1.38", defenseStrength: "0.70" },
  ];

  const allTeams = [...plTeams, ...laLigaTeams, ...bundesligaTeams, ...serieATeams];
  const createdTeams = await db.insert(teams).values(allTeams).returning();
  console.log("✅ Teams created");

  // Create Bookmakers
  const bookmakerData = [
    { name: "Bet365", trustRating: 95, commissionPercent: "2.5" },
    { name: "Pinnacle", trustRating: 98, commissionPercent: "1.8" },
    { name: "William Hill", trustRating: 92, commissionPercent: "3.0" },
    { name: "Unibet", trustRating: 90, commissionPercent: "2.8" },
    { name: "Betfair", trustRating: 94, commissionPercent: "2.0" },
    { name: "1xBet", trustRating: 85, commissionPercent: "3.5" },
  ];

  const createdBookmakers = await db.insert(bookmakers).values(bookmakerData).returning();
  console.log("✅ Bookmakers created");

  // Create Matches
  const now = new Date();
  const matchData = [
    {
      leagueId: createdLeagues[0].leagueId,
      seasonId: currentSeason.seasonId,
      homeTeamId: createdTeams[0].teamId, // Arsenal
      awayTeamId: createdTeams[1].teamId, // Man City
      venueId: createdVenues[2].venueId,
      refereeId: createdReferees[0].refereeId,
      kickoffTime: new Date(now.getTime() + 2 * 60 * 60 * 1000), // 2 hours from now
      status: "Scheduled",
    },
    {
      leagueId: createdLeagues[0].leagueId,
      seasonId: currentSeason.seasonId,
      homeTeamId: createdTeams[2].teamId, // Liverpool
      awayTeamId: createdTeams[3].teamId, // Man Utd
      venueId: createdVenues[1].venueId,
      refereeId: createdReferees[1].refereeId,
      kickoffTime: new Date(now.getTime() + 4 * 60 * 60 * 1000), // 4 hours from now
      status: "Scheduled",
    },
    {
      leagueId: createdLeagues[1].leagueId,
      seasonId: currentSeason.seasonId,
      homeTeamId: createdTeams[8].teamId, // Real Madrid
      awayTeamId: createdTeams[9].teamId, // Barcelona
      venueId: createdVenues[4].venueId,
      refereeId: createdReferees[2].refereeId,
      kickoffTime: new Date(now.getTime() + 6 * 60 * 60 * 1000), // 6 hours from now
      status: "Scheduled",
    },
    {
      leagueId: createdLeagues[2].leagueId,
      seasonId: currentSeason.seasonId,
      homeTeamId: createdTeams[12].teamId, // Bayern
      awayTeamId: createdTeams[13].teamId, // Dortmund
      venueId: createdVenues[6].venueId,
      refereeId: createdReferees[3].refereeId,
      kickoffTime: new Date(now.getTime() + 8 * 60 * 60 * 1000), // 8 hours from now
      status: "Scheduled",
    },
    {
      leagueId: createdLeagues[0].leagueId,
      seasonId: currentSeason.seasonId,
      homeTeamId: createdTeams[4].teamId, // Chelsea
      awayTeamId: createdTeams[5].teamId, // Tottenham
      venueId: createdVenues[2].venueId,
      refereeId: createdReferees[0].refereeId,
      kickoffTime: new Date(now.getTime() + 10 * 60 * 60 * 1000), // 10 hours from now
      status: "Scheduled",
    },
  ];

  const createdMatches = await db.insert(matches).values(matchData).returning();
  console.log("✅ Matches created");

  // Create Predictions (Dixon-Coles model outputs)
  const predictionData = [
    { matchId: createdMatches[0].matchId, probHome: "0.4250", probDraw: "0.2850", probAway: "0.2900", xgHome: "1.65", xgAway: "1.42" },
    { matchId: createdMatches[1].matchId, probHome: "0.5120", probDraw: "0.2680", probAway: "0.2200", xgHome: "1.82", xgAway: "1.15" },
    { matchId: createdMatches[2].matchId, probHome: "0.4580", probDraw: "0.2720", probAway: "0.2700", xgHome: "1.72", xgAway: "1.48" },
    { matchId: createdMatches[3].matchId, probHome: "0.5650", probDraw: "0.2450", probAway: "0.1900", xgHome: "2.05", xgAway: "1.05" },
    { matchId: createdMatches[4].matchId, probHome: "0.4380", probDraw: "0.2920", probAway: "0.2700", xgHome: "1.58", xgAway: "1.38" },
  ];

  await db.insert(predictions).values(predictionData);
  console.log("✅ Predictions created");

  // Create Market Odds
  const oddsData = [];
  for (const match of createdMatches) {
    // Create odds for 1X2 market from different bookmakers
    oddsData.push(
      { matchId: match.matchId, bookieId: createdBookmakers[0].bookieId, marketType: "1X2", selection: "Home", odds: "2.35" },
      { matchId: match.matchId, bookieId: createdBookmakers[0].bookieId, marketType: "1X2", selection: "Draw", odds: "3.50" },
      { matchId: match.matchId, bookieId: createdBookmakers[0].bookieId, marketType: "1X2", selection: "Away", odds: "3.45" },
      { matchId: match.matchId, bookieId: createdBookmakers[1].bookieId, marketType: "1X2", selection: "Home", odds: "2.42" },
      { matchId: match.matchId, bookieId: createdBookmakers[1].bookieId, marketType: "1X2", selection: "Draw", odds: "3.40" },
      { matchId: match.matchId, bookieId: createdBookmakers[1].bookieId, marketType: "1X2", selection: "Away", odds: "3.38" },
    );
  }

  await db.insert(marketOdds).values(oddsData);
  console.log("✅ Market odds created");

  // Create Opportunities (Value bets based on predictions vs odds)
  const opportunityData = [
    {
      matchId: createdMatches[0].matchId,
      type: "Value",
      description: "Arsenal underpriced - True prob 42.5% vs Implied 42.6% (2.35 odds)",
      bookieAId: createdBookmakers[0].bookieId,
      oddsA: "2.35",
      bookieBId: null,
      oddsB: null,
      profitMarginPercent: "4.85",
      status: "Active",
    },
    {
      matchId: createdMatches[1].matchId,
      type: "Value",
      description: "Liverpool strong value - True prob 51.2% vs market odds 2.05",
      bookieAId: createdBookmakers[1].bookieId,
      oddsA: "2.05",
      bookieBId: null,
      oddsB: null,
      profitMarginPercent: "8.12",
      status: "Active",
    },
    {
      matchId: createdMatches[3].matchId,
      type: "Arbitrage",
      description: "Bayern Munich - Arbitrage opportunity detected across bookmakers",
      bookieAId: createdBookmakers[0].bookieId,
      oddsA: "1.78",
      bookieBId: createdBookmakers[5].bookieId,
      oddsB: "5.20",
      profitMarginPercent: "2.45",
      status: "Active",
    },
  ];

  await db.insert(opportunities).values(opportunityData);
  console.log("✅ Opportunities created");

  console.log("✅ Database seeded successfully!");
}

seed()
  .catch((error) => {
    console.error("❌ Seeding failed:", error);
    process.exit(1);
  })
  .then(() => {
    process.exit(0);
  });
