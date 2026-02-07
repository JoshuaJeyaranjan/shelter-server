import express from "express";
import * as locationsController from "../controllers/locationsController.js";

const router = express.Router();

router.get("/", locationsController.getAllLocations);

router.get("/map", locationsController.getLocationsForMap);

router.get("/:id/occupancy", locationsController.getLocationOccupancy);

router.get("/:id", locationsController.getLocationById);

export default router;
