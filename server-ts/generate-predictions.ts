import { db } from "./db";
import { matches, teams, predictions } from "@shared/schema";
import { eq } from "drizzle-orm";
import { predictMatch, calculateExpectedGoals } from "./dixon-coles";

/**
 * Generate predictions for all upcoming matches without predictions
 */
async function generatePredictions() {
  console.log("🔮 Generating predictions using Dixon-Coles model...");

  // Get all matches that don't have predictions yet
  const allMatches = await db.select().from(matches);
  const existingPredictions = await db.select().from(predictions);
  const predictedMatchIds = new Set(existingPredictions.map(p => p.matchId));

  const matchesToPredict = allMatches.filter(m => !predictedMatchIds.has(m.matchId));

  console.log(`Found ${matchesToPredict.length} matches needing predictions`);

  for (const match of matchesToPredict) {
    if (!match.homeTeamId || !match.awayTeamId) continue;

    // Get team data
    const homeTeam = await db.select().from(teams).where(eq(teams.teamId, match.homeTeamId)).limit(1);
    const awayTeam = await db.select().from(teams).where(eq(teams.teamId, match.awayTeamId)).limit(1);

    if (!homeTeam[0] || !awayTeam[0]) continue;

    // Extract attack and defense strengths (with defaults if not set)
    const homeAttack = parseFloat(homeTeam[0].attackStrength || "1.0");
    const homeDefense = parseFloat(homeTeam[0].defenseStrength || "1.0");
    const awayAttack = parseFloat(awayTeam[0].attackStrength || "1.0");
    const awayDefense = parseFloat(awayTeam[0].defenseStrength || "1.0");

    // Calculate expected goals
    const leagueAvg = 1.5; // Premier League average ~1.5 goals per team per match
    const homeAdvantage = 1.15; // Home teams score ~15% more on average
    
    const xgHome = calculateExpectedGoals(homeAttack, awayDefense, leagueAvg, homeAdvantage);
    const xgAway = calculateExpectedGoals(awayAttack, homeDefense, leagueAvg, 1.0);

    // Run Dixon-Coles prediction
    const prediction = predictMatch(xgHome, xgAway);

    // Store prediction
    await db.insert(predictions).values({
      matchId: match.matchId,
      probHome: prediction.probHome.toFixed(4),
      probDraw: prediction.probDraw.toFixed(4),
      probAway: prediction.probAway.toFixed(4),
      xgHome: prediction.xgHome.toFixed(2),
      xgAway: prediction.xgAway.toFixed(2),
    });

    console.log(`✅ Predicted: ${homeTeam[0].name} vs ${awayTeam[0].name}`);
    console.log(`   Home: ${(prediction.probHome * 100).toFixed(1)}% | Draw: ${(prediction.probDraw * 100).toFixed(1)}% | Away: ${(prediction.probAway * 100).toFixed(1)}%`);
    console.log(`   xG: ${xgHome.toFixed(2)} - ${xgAway.toFixed(2)}`);
  }

  console.log("✅ Predictions generated successfully!");
}

generatePredictions()
  .catch((error) => {
    console.error("❌ Prediction generation failed:", error);
    process.exit(1);
  })
  .then(() => {
    process.exit(0);
  });
