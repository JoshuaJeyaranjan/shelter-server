import express from "express";
import * as locationsController from "../controllers/locationsController.js";

const router = express.Router();

// Return all locations, optionally filtered by query params
router.get("/", locationsController.getAllLocations);

// Return data for mapping (lat/lng + basic info)
router.get("/map", locationsController.getLocationsForMap);

// Occupancy info per location
router.get("/:id/occupancy", locationsController.getLocationOccupancy);

// Route for a single location's full info
router.get("/:id", locationsController.getLocationById);

export default router;