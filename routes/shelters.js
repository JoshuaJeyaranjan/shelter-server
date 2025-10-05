const express = require("express");
const router = express.Router();
const sheltersController = require("../controllers/sheltersController");

// Return all shelters, optionally filtered by query params
router.get("/", sheltersController.getAllShelters);

// Return data for mapping (lat/lng + basic info)
router.get("/map", sheltersController.getSheltersForMap);

router.get("/metadata", sheltersController.getSheltersMetadata); 

// Route for a shelter's location
router.get("/:id/location", sheltersController.getShelterLocation);

// Occupancy info per shelter
router.get("/:id/occupancy", sheltersController.getShelterOccupancy);


// Return a single shelter by ID
router.get("/:id", sheltersController.getShelterById);


module.exports = router;