const pool = require("../config/db");

// GET /api/shelters

// GET /api/shelters
exports.getAllShelters = async (req, res, next) => {
  try {
    const { sector, city, minVacancyBeds, minVacancyRooms } = req.query;

    // Step 1: fetch shelters with optional filters
    let query = "SELECT * FROM shelters";
    const conditions = [];
    const params = [];

    if (sector) {
      params.push(sector);
      conditions.push(`sector = $${params.length}`);
    }
    if (city) {
      params.push(city);
      conditions.push(`city = $${params.length}`);
    }
    if (minVacancyBeds) {
      params.push(Number(minVacancyBeds));
      conditions.push(`(capacity_actual_bed - COALESCE(occupied_beds,0)) >= $${params.length}`);
    }
    if (minVacancyRooms) {
      params.push(Number(minVacancyRooms));
      conditions.push(`(capacity_actual_room - COALESCE(occupied_rooms,0)) >= $${params.length}`);
    }
    if (conditions.length > 0) {
      query += " WHERE " + conditions.join(" AND ");
    }

    const { rows } = await pool.query(query, params);

    // Step 2: deduplicate by location (address + name + city + province)
    const locationsMap = new Map();
    rows.forEach(s => {
      const key = `${s.location_name}||${s.address}||${s.city}||${s.province}`;
      if (!locationsMap.has(key)) {
        locationsMap.set(key, {
          location_name: s.location_name,
          address: s.address,
          city: s.city,
          province: s.province,
          latitude: s.latitude,
          longitude: s.longitude,
          programs: []
        });
      }
      locationsMap.get(key).programs.push({
        id: s.id,
        sector: s.sector,
        program_name: s.program_name,
        capacity_actual_bed: s.capacity_actual_bed,
        capacity_actual_room: s.capacity_actual_room,
        occupied_beds: s.occupied_beds,
        occupied_rooms: s.occupied_rooms
      });
    });

    res.json(Array.from(locationsMap.values()));
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/:id
exports.getShelterById = async (req, res, next) => {
  try {
    const { id } = req.params;

    // Step 1: get the shelter by id
    const shelterResult = await pool.query("SELECT * FROM shelters WHERE id = $1", [id]);
    if (shelterResult.rows.length === 0) return res.status(404).json({ message: "Shelter not found" });

    const shelter = shelterResult.rows[0];

    // Step 2: get all programs for the same location
    const programsResult = await pool.query(
      `SELECT * FROM shelters
       WHERE location_name = $1 AND address = $2 AND city = $3 AND province = $4`,
      [shelter.location_name, shelter.address, shelter.city, shelter.province]
    );

    const locationData = {
      location_name: shelter.location_name,
      address: shelter.address,
      city: shelter.city,
      province: shelter.province,
      latitude: shelter.latitude,
      longitude: shelter.longitude,
      programs: programsResult.rows.map(s => ({
        id: s.id,
        sector: s.sector,
        program_name: s.program_name,
        capacity_actual_bed: s.capacity_actual_bed,
        capacity_actual_room: s.capacity_actual_room,
        occupied_beds: s.occupied_beds,
        occupied_rooms: s.occupied_rooms
      }))
    };

    res.json(locationData);
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

// GET /api/shelters/metadata
exports.getSheltersMetadata = async (req, res, next) => {
  try {
    // Pass the value for $1
    const result = await pool.query("SELECT * FROM shelter_metadata WHERE id = $1", [1]);
    
    if (!result.rows[0]) {
      return res.json({ lastRefreshed: null });
    }

    const metadata = {
      lastRefreshed: result.rows[0].last_refreshed,
    };

    res.json(metadata);
  } catch (err) {
    next(err);
  }
};