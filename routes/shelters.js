const express = require("express");
const router = express.Router();
const sheltersController = require("../controllers/sheltersController");

// Return all shelters, optionally filtered by query params
router.get("/", sheltersController.getAllShelters);

// Return a single shelter by ID
router.get("/:id", sheltersController.getShelterById);

// Return data for mapping (lat/lng + basic info)
router.get("/map", sheltersController.getSheltersForMap);

module.exports = router;