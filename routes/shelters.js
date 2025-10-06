const express = require("express");
const router = express.Router();
const locationsController = require("../controllers/locationsController");

// Return all locations, optionally filtered by query params
router.get("/", locationsController.getAllLocations);

// Return data for mapping (lat/lng + basic info)
router.get("/map", locationsController.getLocationsForMap);

// Return metadata
router.get("/metadata", locationsController.getSheltersMetadata);

// Route for a location's basic info
router.get("/:id/location", locationsController.getLocationById);

// Occupancy info per location
router.get("/:id/occupancy", locationsController.getLocationOccupancy);

module.exports = router;