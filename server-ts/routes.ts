import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { insertMatchSchema, insertOpportunitySchema, insertPredictionSchema } from "@shared/schema";
import { z } from "zod";
import { fromZodError } from "zod-validation-error";

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  
  // GET /api/matches - Get all matches
  app.get("/api/matches", async (req, res) => {
    try {
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 50;
      const matches = await storage.getMatches(limit);
      res.json(matches);
    } catch (error) {
      console.error("Error fetching matches:", error);
      res.status(500).json({ error: "Failed to fetch matches" });
    }
  });

  // GET /api/matches/upcoming - Get upcoming matches
  app.get("/api/matches/upcoming", async (req, res) => {
    try {
      const matches = await storage.getUpcomingMatches();
      res.json(matches);
    } catch (error) {
      console.error("Error fetching upcoming matches:", error);
      res.status(500).json({ error: "Failed to fetch upcoming matches" });
    }
  });

  // GET /api/matches/:id - Get match by ID
  app.get("/api/matches/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const match = await storage.getMatchById(id);
      if (!match) {
        return res.status(404).json({ error: "Match not found" });
      }
      res.json(match);
    } catch (error) {
      console.error("Error fetching match:", error);
      res.status(500).json({ error: "Failed to fetch match" });
    }
  });

  // POST /api/matches - Create a new match
  app.post("/api/matches", async (req, res) => {
    try {
      const validation = insertMatchSchema.safeParse(req.body);
      if (!validation.success) {
        return res.status(400).json({ error: fromZodError(validation.error).message });
      }
      const match = await storage.createMatch(validation.data);
      res.status(201).json(match);
    } catch (error) {
      console.error("Error creating match:", error);
      res.status(500).json({ error: "Failed to create match" });
    }
  });

  // GET /api/teams - Get all teams
  app.get("/api/teams", async (req, res) => {
    try {
      const teams = await storage.getTeams();
      res.json(teams);
    } catch (error) {
      console.error("Error fetching teams:", error);
      res.status(500).json({ error: "Failed to fetch teams" });
    }
  });

  // GET /api/teams/:id - Get team by ID
  app.get("/api/teams/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const team = await storage.getTeamById(id);
      if (!team) {
        return res.status(404).json({ error: "Team not found" });
      }
      res.json(team);
    } catch (error) {
      console.error("Error fetching team:", error);
      res.status(500).json({ error: "Failed to fetch team" });
    }
  });

  // GET /api/leagues - Get all leagues
  app.get("/api/leagues", async (req, res) => {
    try {
      const leagues = await storage.getLeagues();
      res.json(leagues);
    } catch (error) {
      console.error("Error fetching leagues:", error);
      res.status(500).json({ error: "Failed to fetch leagues" });
    }
  });

  // GET /api/bookmakers - Get all bookmakers
  app.get("/api/bookmakers", async (req, res) => {
    try {
      const bookmakers = await storage.getBookmakers();
      res.json(bookmakers);
    } catch (error) {
      console.error("Error fetching bookmakers:", error);
      res.status(500).json({ error: "Failed to fetch bookmakers" });
    }
  });

  // GET /api/opportunities - Get active opportunities
  app.get("/api/opportunities", async (req, res) => {
    try {
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 20;
      const opportunities = await storage.getActiveOpportunities(limit);
      res.json(opportunities);
    } catch (error) {
      console.error("Error fetching opportunities:", error);
      res.status(500).json({ error: "Failed to fetch opportunities" });
    }
  });

  // POST /api/opportunities - Create a new opportunity
  app.post("/api/opportunities", async (req, res) => {
    try {
      const validation = insertOpportunitySchema.safeParse(req.body);
      if (!validation.success) {
        return res.status(400).json({ error: fromZodError(validation.error).message });
      }
      const opportunity = await storage.createOpportunity(validation.data);
      res.status(201).json(opportunity);
    } catch (error) {
      console.error("Error creating opportunity:", error);
      res.status(500).json({ error: "Failed to create opportunity" });
    }
  });

  // GET /api/predictions/:matchId - Get prediction for a match
  app.get("/api/predictions/:matchId", async (req, res) => {
    try {
      const matchId = parseInt(req.params.matchId);
      const prediction = await storage.getPredictionForMatch(matchId);
      if (!prediction) {
        return res.status(404).json({ error: "Prediction not found" });
      }
      res.json(prediction);
    } catch (error) {
      console.error("Error fetching prediction:", error);
      res.status(500).json({ error: "Failed to fetch prediction" });
    }
  });

  // POST /api/predictions - Create a new prediction
  app.post("/api/predictions", async (req, res) => {
    try {
      const validation = insertPredictionSchema.safeParse(req.body);
      if (!validation.success) {
        return res.status(400).json({ error: fromZodError(validation.error).message });
      }
      const prediction = await storage.createPrediction(validation.data);
      res.status(201).json(prediction);
    } catch (error) {
      console.error("Error creating prediction:", error);
      res.status(500).json({ error: "Failed to create prediction" });
    }
  });

  // GET /api/odds/:matchId - Get odds for a match
  app.get("/api/odds/:matchId", async (req, res) => {
    try {
      const matchId = parseInt(req.params.matchId);
      const odds = await storage.getOddsForMatch(matchId);
      res.json(odds);
    } catch (error) {
      console.error("Error fetching odds:", error);
      res.status(500).json({ error: "Failed to fetch odds" });
    }
  });

  // POST /api/predictions/generate - Generate predictions for a match
  app.post("/api/predictions/generate", async (req, res) => {
    try {
      const { matchId } = req.body;
      if (!matchId) {
        return res.status(400).json({ error: "matchId is required" });
      }

      // This would trigger the prediction generation for a specific match
      // For now, return success - full implementation would call the Dixon-Coles model
      res.status(200).json({ message: "Prediction generation queued" });
    } catch (error) {
      console.error("Error generating prediction:", error);
      res.status(500).json({ error: "Failed to generate prediction" });
    }
  });

  return httpServer;
}
