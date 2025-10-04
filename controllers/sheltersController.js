const pool = require("../config/db");

// GET /api/shelters
exports.getAllShelters = async (req, res, next) => {
  try {
    const { sector, city, minVacancyBeds, minVacancyRooms } = req.query;

    let query = "SELECT * FROM shelters";
    const conditions = [];
    const params = [];

    // Sector filter
    if (sector) {
      params.push(sector);
      conditions.push(`sector = $${params.length}`);
    }

    // City filter
    if (city) {
      params.push(city);
      conditions.push(`city = $${params.length}`);
    }

    // Minimum vacancy for beds
    if (minVacancyBeds) {
      params.push(Number(minVacancyBeds));
      conditions.push(`(capacity_actual_bed - COALESCE(occupied_beds,0)) >= $${params.length}`);
    }

    // Minimum vacancy for rooms
    if (minVacancyRooms) {
      params.push(Number(minVacancyRooms));
      conditions.push(`(capacity_actual_room - COALESCE(occupied_rooms,0)) >= $${params.length}`);
    }

    if (conditions.length > 0) {
      query += " WHERE " + conditions.join(" AND ");
    }

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
};
// GET /api/shelters/:id
exports.getShelterById = async (req, res, next) => {
  try {
    const { id } = req.params;
    const result = await pool.query("SELECT * FROM shelters WHERE id = $1", [id]);
    if (result.rows.length === 0) return res.status(404).json({ message: "Shelter not found" });
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/map
exports.getSheltersForMap = async (req, res, next) => {
  try {
    const result = await pool.query(
      "SELECT id, location_name, address, city, province FROM shelters"
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/:id/location
exports.getShelterLocation = async (req, res, next) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT id, location_name, address, city, province 
       FROM shelters 
       WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Shelter not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/:id/occupancy
exports.getShelterOccupancy = async (req, res, next) => {
  try {
    const { id } = req.params;

    const result = await pool.query(
      `SELECT 
         id,
         location_name,
         capacity_actual_bed,
         capacity_actual_room,
         occupied_beds,
         unoccupied_beds,
         occupied_rooms,
         unoccupied_rooms,
         occupancy_date
       FROM shelters
       WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Shelter not found" });
    }

    const shelter = result.rows[0];

    // Optional: calculate occupancy rates if not already stored
    const occupancyRateBeds = shelter.capacity_actual_bed
      ? ((shelter.occupied_beds || 0) / shelter.capacity_actual_bed) * 100
      : null;

    const occupancyRateRooms = shelter.capacity_actual_room
      ? ((shelter.occupied_rooms || 0) / shelter.capacity_actual_room) * 100
      : null;

    res.json({
      id: shelter.id,
      location_name: shelter.location_name,
      capacity_actual_bed: shelter.capacity_actual_bed,
      capacity_actual_room: shelter.capacity_actual_room,
      occupied_beds: shelter.occupied_beds,
      unoccupied_beds: shelter.unoccupied_beds,
      occupied_rooms: shelter.occupied_rooms,
      unoccupied_rooms: shelter.unoccupied_rooms,
      occupancy_rate_beds: occupancyRateBeds,
      occupancy_rate_rooms: occupancyRateRooms,
      occupancy_date: shelter.occupancy_date,
    });
  } catch (err) {
    next(err);
  }
};