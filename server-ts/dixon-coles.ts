/**
 * Dixon-Coles Poisson Model Implementation
 * 
 * This implements the Dixon-Coles adjustment to the Poisson model for football match predictions.
 * The model calculates "true probabilities" by:
 * 1. Using Poisson distribution for goal scoring
 * 2. Applying Dixon-Coles correction for low-scoring draws (addresses correlation)
 * 3. Deriving match outcome probabilities from score matrix
 */

/**
 * Calculate Poisson probability: P(X = k) = (λ^k * e^-λ) / k!
 */
function poissonProbability(lambda: number, k: number): number {
  if (k < 0) return 0;
  
  // Using log space for numerical stability with large values
  const logProb = k * Math.log(lambda) - lambda - logFactorial(k);
  return Math.exp(logProb);
}

/**
 * Calculate log(k!) using Stirling's approximation for large k
 */
function logFactorial(k: number): number {
  if (k <= 1) return 0;
  if (k < 20) {
    let result = 0;
    for (let i = 2; i <= k; i++) {
      result += Math.log(i);
    }
    return result;
  }
  // Stirling's approximation: ln(n!) ≈ n*ln(n) - n + 0.5*ln(2πn)
  return k * Math.log(k) - k + 0.5 * Math.log(2 * Math.PI * k);
}

/**
 * Dixon-Coles tau function - correlation adjustment for low scores
 * Addresses the fact that low-scoring draws occur more frequently than independent Poisson would predict
 * 
 * @param homeGoals - Home team goals in the scoreline
 * @param awayGoals - Away team goals in the scoreline
 * @param rho - Correlation parameter (typically around -0.13)
 */
function dixonColesTau(homeGoals: number, awayGoals: number, rho: number): number {
  if (homeGoals === 0 && awayGoals === 0) return 1 - rho;
  if (homeGoals === 0 && awayGoals === 1) return 1 + rho;
  if (homeGoals === 1 && awayGoals === 0) return 1 + rho;
  if (homeGoals === 1 && awayGoals === 1) return 1 - rho;
  return 1; // No adjustment for other scores
}

/**
 * Generate the full probability matrix for all possible scores up to maxGoals x maxGoals
 * 
 * @param lambdaHome - Expected goals for home team (Poisson parameter)
 * @param lambdaAway - Expected goals for away team (Poisson parameter)
 * @param maxGoals - Maximum goals to calculate (default 10)
 * @param rho - Dixon-Coles correlation parameter (default -0.13)
 */
export function calculateScoreMatrix(
  lambdaHome: number,
  lambdaAway: number,
  maxGoals: number = 10,
  rho: number = -0.13
): number[][] {
  const matrix: number[][] = [];
  
  // First pass: Calculate raw Poisson probabilities
  for (let home = 0; home <= maxGoals; home++) {
    matrix[home] = [];
    for (let away = 0; away <= maxGoals; away++) {
      const probHome = poissonProbability(lambdaHome, home);
      const probAway = poissonProbability(lambdaAway, away);
      matrix[home][away] = probHome * probAway;
    }
  }
  
  // Second pass: Apply Dixon-Coles correction
  for (let home = 0; home <= maxGoals; home++) {
    for (let away = 0; away <= maxGoals; away++) {
      const tau = dixonColesTau(home, away, rho);
      matrix[home][away] *= tau;
    }
  }
  
  // Normalize to ensure probabilities sum to 1
  const total = matrix.flat().reduce((sum, prob) => sum + prob, 0);
  for (let home = 0; home <= maxGoals; home++) {
    for (let away = 0; away <= maxGoals; away++) {
      matrix[home][away] /= total;
    }
  }
  
  return matrix;
}

export interface MatchPrediction {
  probHome: number;
  probDraw: number;
  probAway: number;
  xgHome: number;
  xgAway: number;
  prob0_0: number;
  probOver2_5: number;
  probBTTS: number;
}

/**
 * Calculate match outcome probabilities from the score matrix
 * 
 * @param lambdaHome - Expected goals for home team
 * @param lambdaAway - Expected goals for away team
 */
export function predictMatch(lambdaHome: number, lambdaAway: number): MatchPrediction {
  const matrix = calculateScoreMatrix(lambdaHome, lambdaAway);
  const maxGoals = matrix.length - 1;
  
  let probHome = 0;
  let probDraw = 0;
  let probAway = 0;
  let prob0_0 = matrix[0][0];
  let probOver2_5 = 0;
  let probBTTS = 0;
  
  // Sum probabilities across the matrix
  for (let home = 0; home <= maxGoals; home++) {
    for (let away = 0; away <= maxGoals; away++) {
      const prob = matrix[home][away];
      
      // Match result
      if (home > away) probHome += prob;
      else if (home === away) probDraw += prob;
      else probAway += prob;
      
      // Over 2.5 goals (total goals > 2.5, i.e., >= 3)
      if ((home + away) > 2.5) probOver2_5 += prob;
      
      // Both teams to score
      if (home > 0 && away > 0) probBTTS += prob;
    }
  }
  
  return {
    probHome,
    probDraw,
    probAway,
    xgHome: lambdaHome,
    xgAway: lambdaAway,
    prob0_0,
    probOver2_5,
    probBTTS,
  };
}

/**
 * Calculate expected goals (lambda) for a team based on their attacking/defensive strength
 * 
 * Formula: λ = Attack_Strength × Defense_Strength_Opponent × League_Average × Home_Advantage
 * 
 * @param attackStrength - Team's attack strength (goals scored / league avg)
 * @param defenseStrengthOpp - Opponent's defense strength (goals conceded / league avg)
 * @param leagueAvg - League average goals per game
 * @param homeAdvantage - Home advantage multiplier (typically 1.0-1.3)
 */
export function calculateExpectedGoals(
  attackStrength: number,
  defenseStrengthOpp: number,
  leagueAvg: number = 1.5,
  homeAdvantage: number = 1.0
): number {
  return attackStrength * defenseStrengthOpp * leagueAvg * homeAdvantage;
}

/**
 * Identify value bets by comparing true probability vs market implied probability
 * 
 * @param trueProbability - Model's calculated probability
 * @param odds - Bookmaker's decimal odds
 * @param margin - Minimum edge required (as decimal, e.g., 0.05 for 5%)
 */
export function findValueBet(
  trueProbability: number,
  odds: number,
  margin: number = 0.05
): { hasValue: boolean; edge: number; fairOdds: number } {
  const impliedProbability = 1 / odds;
  const fairOdds = 1 / trueProbability;
  const edge = (trueProbability - impliedProbability) / impliedProbability;
  
  return {
    hasValue: edge > margin,
    edge: edge * 100, // Convert to percentage
    fairOdds,
  };
}

/**
 * Detect arbitrage opportunities across multiple bookmakers
 * 
 * @param odds1 - Odds from bookmaker 1
 * @param odds2 - Odds from bookmaker 2
 * @param odds3 - Odds from bookmaker 3 (optional, for 3-way markets like 1X2)
 */
export function detectArbitrage(
  odds1: number,
  odds2: number,
  odds3?: number
): { isArbitrage: boolean; profitMargin: number } {
  const probs = [1 / odds1, 1 / odds2];
  if (odds3) probs.push(1 / odds3);
  
  const totalImplied = probs.reduce((sum, prob) => sum + prob, 0);
  const profitMargin = (1 / totalImplied - 1) * 100;
  
  return {
    isArbitrage: totalImplied < 1,
    profitMargin,
  };
}
