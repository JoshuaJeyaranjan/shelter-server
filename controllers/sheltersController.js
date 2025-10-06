const pool = require("../config/db");

// GET /api/shelters

// GET /api/shelters
exports.getAllShelters = async (req, res, next) => {
  try {
    const { sector, city, minVacancyBeds, minVacancyRooms } = req.query;

    // Step 1: build the query with optional filters
    let query = "SELECT * FROM shelters";
    const conditions = ["address IS NOT NULL"]; // only include shelters with addresses
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

    // Step 2: group programs by location, filtering out invalid programs
    const locationsMap = new Map();

    rows.forEach(row => {
      if (!row.program_name || !row.sector) return; // skip invalid programs

      const key = `${row.location_name}||${row.address}||${row.city}||${row.province}`;
      if (!locationsMap.has(key)) {
        locationsMap.set(key, {
          location_name: row.location_name,
          address: row.address,
          city: row.city,
          province: row.province,
          latitude: row.latitude,
          longitude: row.longitude,
          programs: []
        });
      }

      const location = locationsMap.get(key);

      location.programs.push({
        id: row.id,
        program_name: row.program_name,
        sector: row.sector,
        overnight_service_type: row.overnight_service_type,
        service_user_count: row.service_user_count,
        capacity_actual_bed: row.capacity_actual_bed,
        occupied_beds: row.occupied_beds,
        unoccupied_beds: row.unoccupied_beds,
        capacity_actual_room: row.capacity_actual_room,
        occupied_rooms: row.occupied_rooms,
        unoccupied_rooms: row.unoccupied_rooms,
        occupancy_date: row.occupancy_date
      });
    });

    // Step 3: remove locations that ended up with no programs
    const filteredLocations = Array.from(locationsMap.values()).filter(loc => loc.programs.length > 0);

    res.json({ locations: filteredLocations });
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/:id
exports.getShelterById = async (req, res, next) => {
  try {
    const { id } = req.params;

    // Step 1: fetch the shelter row by id
    const shelterResult = await pool.query("SELECT * FROM shelters WHERE id = $1 AND address IS NOT NULL", [id]);
    if (shelterResult.rows.length === 0) return res.status(404).json({ message: "Shelter not found" });

    const shelter = shelterResult.rows[0];

    // Step 2: fetch all valid programs for this location
    const programsResult = await pool.query(
      `SELECT * FROM shelters 
       WHERE location_name = $1 AND address = $2 AND city = $3 AND province = $4
       AND program_name IS NOT NULL AND sector IS NOT NULL`,
      [shelter.location_name, shelter.address, shelter.city, shelter.province]
    );

    // Step 3: construct the location object
    const locationData = {
      location_name: shelter.location_name,
      address: shelter.address,
      city: shelter.city,
      province: shelter.province,
      latitude: shelter.latitude,
      longitude: shelter.longitude,
      programs: programsResult.rows.map(p => ({
        id: p.id,
        program_name: p.program_name,
        sector: p.sector,
        overnight_service_type: p.overnight_service_type,
        service_user_count: p.service_user_count,
        capacity_actual_bed: p.capacity_actual_bed,
        occupied_beds: p.occupied_beds,
        unoccupied_beds: p.unoccupied_beds,
        capacity_actual_room: p.capacity_actual_room,
        occupied_rooms: p.occupied_rooms,
        unoccupied_rooms: p.unoccupied_rooms,
        occupancy_date: p.occupancy_date
      }))
    };

    res.json(locationData);
  } catch (err) {
    next(err);
  }
};



// GET /api/shelters/map
exports.getSheltersForMap = async (req, res, next) => {
  try {
    const result = await pool.query(
      "SELECT location_name, address, city, province, latitude, longitude FROM shelters"
    );

    const locationsMap = new Map();
    result.rows.forEach(row => {
      const key = `${row.location_name}||${row.address}||${row.city}||${row.province}`;
      if (!locationsMap.has(key)) {
        locationsMap.set(key, row);
      }
    });

    res.json(Array.from(locationsMap.values()));
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
// GET /api/shelters/:id/occupancy
exports.getShelterOccupancy = async (req, res, next) => {
  try {
    const { id } = req.params;

    const shelterResult = await pool.query("SELECT * FROM shelters WHERE id = $1", [id]);
    if (shelterResult.rows.length === 0) return res.status(404).json({ message: "Shelter not found" });

    const shelter = shelterResult.rows[0];

    const programsResult = await pool.query(
      `SELECT * FROM shelters 
       WHERE location_name = $1 AND address = $2 AND city = $3 AND province = $4`,
      [shelter.location_name, shelter.address, shelter.city, shelter.province]
    );

    const programs = programsResult.rows.map(p => {
      const occupancyRateBeds = p.capacity_actual_bed ? ((p.occupied_beds || 0) / p.capacity_actual_bed) * 100 : null;
      const occupancyRateRooms = p.capacity_actual_room ? ((p.occupied_rooms || 0) / p.capacity_actual_room) * 100 : null;
      return {
        id: p.id,
        program_name: p.program_name,
        capacity_actual_bed: p.capacity_actual_bed,
        occupied_beds: p.occupied_beds,
        unoccupied_beds: p.unoccupied_beds,
        capacity_actual_room: p.capacity_actual_room,
        occupied_rooms: p.occupied_rooms,
        unoccupied_rooms: p.unoccupied_rooms,
        occupancy_rate_beds: occupancyRateBeds,
        occupancy_rate_rooms: occupancyRateRooms,
        occupancy_date: p.occupancy_date
      };
    });

    res.json({
      location_name: shelter.location_name,
      address: shelter.address,
      city: shelter.city,
      province: shelter.province,
      latitude: shelter.latitude,
      longitude: shelter.longitude,
      programs
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