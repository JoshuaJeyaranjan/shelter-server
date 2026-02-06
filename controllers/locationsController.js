const pool = require("../config/db");

// GET /api/locations
exports.getAllLocations = async (req, res, next) => {
  try {
    const { sector, city, minVacancyBeds, minVacancyRooms } = req.query;

    // Fetch locations
const locationsResult = await pool.query(
  `
  SELECT
    id,
    location_name,
    address,
    city,
    province,
    latitude,
    longitude,
    shelter_type,
    organization_name,
    last_refreshed
  FROM locations
  WHERE address IS NOT NULL
  ${city ? "AND city = $1" : ""}
  `,
  city ? [city] : []
);
    const locations = locationsResult.rows;

    if (!locations.length) return res.json({ locations: [] });

    // Fetch all programs for these locations
    const locationIds = locations.map((l) => l.id);
    let programQuery = "SELECT * FROM programs WHERE location_id = ANY($1::int[])";
    const programParams = [locationIds];

    // Optional filters
    const programConditions = [];
    if (sector) {
      programParams.push(sector);
      programConditions.push(`sector = $${programParams.length}`);
    }
    if (minVacancyBeds) {
      programParams.push(Number(minVacancyBeds));
      programConditions.push(
        `(capacity_actual_bed - COALESCE(occupied_beds,0)) >= $${programParams.length}`
      );
    }
    if (minVacancyRooms) {
      programParams.push(Number(minVacancyRooms));
      programConditions.push(
        `(capacity_actual_room - COALESCE(occupied_rooms,0)) >= $${programParams.length}`
      );
    }

    if (programConditions.length) {
      programQuery += " AND " + programConditions.join(" AND ");
    }

    const programsResult = await pool.query(programQuery, programParams);
    const programs = programsResult.rows;

    // Map programs to locations
    const locationsWithPrograms = locations
      .map((loc) => ({
        ...loc,
        programs: programs.filter((p) => p.location_id === loc.id),
      }))
      .filter((loc) => loc.programs.length > 0); // remove locations with no programs

    res.json({ locations: locationsWithPrograms });
  } catch (err) {
    next(err);
  }
};

// GET /api/locations/:id
exports.getLocationById = async (req, res, next) => {
  try {
    const { id } = req.params;

    const locationResult = await pool.query(
      "SELECT * FROM locations WHERE id = $1 AND address IS NOT NULL",
      [id]
    );
    if (!locationResult.rows.length)
      return res.status(404).json({ message: "Location not found" });

    const location = locationResult.rows[0];

    const programsResult = await pool.query(
      "SELECT * FROM programs WHERE location_id = $1",
      [id]
    );

    res.json({ ...location, programs: programsResult.rows });
  } catch (err) {
    next(err);
  }
};

// GET /api/locations/:id/occupancy
exports.getLocationOccupancy = async (req, res, next) => {
  try {
    const { id } = req.params;

    const locationResult = await pool.query(
      "SELECT * FROM locations WHERE id = $1",
      [id]
    );
    if (!locationResult.rows.length)
      return res.status(404).json({ message: "Location not found" });
    const location = locationResult.rows[0];

    const programsResult = await pool.query(
      "SELECT * FROM programs WHERE location_id = $1",
      [id]
    );

    const programs = programsResult.rows.map((p) => ({
      id: p.id,
      program_name: p.program_name,
      capacity_actual_bed: p.capacity_actual_bed,
      occupied_beds: p.occupied_beds,
      unoccupied_beds:
        p.capacity_actual_bed != null && p.occupied_beds != null
          ? p.capacity_actual_bed - p.occupied_beds
          : null,
      capacity_actual_room: p.capacity_actual_room,
      occupied_rooms: p.occupied_rooms,
      unoccupied_rooms:
        p.capacity_actual_room != null && p.occupied_rooms != null
          ? p.capacity_actual_room - p.occupied_rooms
          : null,
      occupancy_rate_beds: p.capacity_actual_bed
        ? ((p.occupied_beds || 0) / p.capacity_actual_bed) * 100
        : null,
      occupancy_rate_rooms: p.capacity_actual_room
        ? ((p.occupied_rooms || 0) / p.capacity_actual_room) * 100
        : null,
      occupancy_date: p.occupancy_date,
    }));

    res.json({ ...location, programs });
  } catch (err) {
    next(err);
  }
};

// GET /api/locations/map
exports.getLocationsForMap = async (req, res, next) => {
  try {
    // 1️⃣ Fetch all locations with their lat/lng and address info
    const locationsResult = await pool.query(`
      SELECT
        id,
        location_name,
        address,
        city,
        province,
        latitude,
        longitude,
        shelter_type,
        last_refreshed
      FROM locations
      WHERE address IS NOT NULL
    `);

    const locations = locationsResult.rows;

    // 2️⃣ Fetch all programs
    const programsResult = await pool.query(`
      SELECT
        id,
        location_id,
        program_name,
        sector,
        capacity_actual_bed,
        occupied_beds,
        capacity_actual_room,
        occupied_rooms,
        occupancy_date
      FROM programs
    `);

    const programs = programsResult.rows;

    // 3️⃣ Merge programs into their corresponding locations
    const locationsWithPrograms = locations.map((loc) => ({
      ...loc,
      programs: programs.filter((p) => p.location_id === loc.id),
    }));

    // 4️⃣ Optional: filter out any locations that somehow have no lat/lng
    const validLocations = locationsWithPrograms.filter(
      (loc) =>
        loc.latitude !== null &&
        loc.latitude !== undefined &&
        loc.longitude !== null &&
        loc.longitude !== undefined
    );

    res.json(validLocations);
  } catch (err) {
    next(err);
  }
};