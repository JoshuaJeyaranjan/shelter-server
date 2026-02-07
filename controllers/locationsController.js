import pool from '../config/db.js';

/**
 * Enrich programs with occupancy info
 */
function enrichPrograms(programs) {
  return programs.map(p => {
    const unoccupiedBeds =
      p.capacity_actual_bed != null && p.occupied_beds != null
        ? p.capacity_actual_bed - p.occupied_beds
        : null;
    const unoccupiedRooms =
      p.capacity_actual_room != null && p.occupied_rooms != null
        ? p.capacity_actual_room - p.occupied_rooms
        : null;

    return {
      id: p.id,
      program_name: p.program_name,
      capacity_actual_bed: p.capacity_actual_bed,
      occupied_beds: p.occupied_beds,
      unoccupied_beds: unoccupiedBeds,
      capacity_actual_room: p.capacity_actual_room,
      occupied_rooms: p.occupied_rooms,
      unoccupied_rooms: unoccupiedRooms,
      occupancy_rate_beds: unoccupiedBeds != null ? (p.occupied_beds / p.capacity_actual_bed) * 100 : null,
      occupancy_rate_rooms: unoccupiedRooms != null ? (p.occupied_rooms / p.capacity_actual_room) * 100 : null,
      sector: p.sector,
      overnight_service_type: p.overnight_service_type,
      occupancy_date: p.occupancy_date,
      location_id: p.location_id,
    };
  });
}

export const getProgramsByLocationId = async (locationId) => {
  const result = await pool.query(`SELECT * FROM programs WHERE location_id = $1`, [locationId]);
  return enrichPrograms(result.rows);
};


export const getAllLocations = async (req, res, next) => {
  try {
    const { sector, city, minVacancyBeds, minVacancyRooms } = req.query;

    // Build dynamic filters
    const conditions = ['address IS NOT NULL'];
    const params = [];
    let paramIndex = 1;

    if (city) {
      conditions.push(`city = $${paramIndex++}`);
      params.push(city);
    }

    const locationsQuery = `
      SELECT id, location_name, address, city, province, latitude, longitude,
             shelter_type, organization_name, last_refreshed
      FROM locations
      WHERE ${conditions.join(' AND ')}
    `;
    const locationsResult = await pool.query(locationsQuery, params);
    const locations = locationsResult.rows;

    if (!locations.length) return res.json({ locations: [] });

    // Fetch programs in one query with optional filters
    const locationIds = locations.map(l => l.id);
    const programConditions = ['location_id = ANY($1::int[])'];
    const programParams = [locationIds];

    if (sector) {
      programParams.push(sector);
      programConditions.push(`sector = $${programParams.length}`);
    }
    if (minVacancyBeds) {
      const beds = Number(minVacancyBeds);
      if (!isNaN(beds)) {
        programParams.push(beds);
        programConditions.push(`(capacity_actual_bed - COALESCE(occupied_beds,0)) >= $${programParams.length}`);
      }
    }
    if (minVacancyRooms) {
      const rooms = Number(minVacancyRooms);
      if (!isNaN(rooms)) {
        programParams.push(rooms);
        programConditions.push(`(capacity_actual_room - COALESCE(occupied_rooms,0)) >= $${programParams.length}`);
      }
    }

    const programsQuery = `SELECT * FROM programs WHERE ${programConditions.join(' AND ')}`;
    const programsResult = await pool.query(programsQuery, programParams);
    const programs = enrichPrograms(programsResult.rows);

    // Map programs to their locations
    const locationsWithPrograms = locations
      .map(loc => ({
        ...loc,
        programs: programs.filter(p => p.location_id === loc.id),
      }))
      .filter(loc => loc.programs.length > 0);

    res.json({ locations: locationsWithPrograms });
  } catch (err) {
    next(err);
  }
};

/**
 * GET single location by ID
 */
export const getLocationById = async (req, res, next) => {
  try {
    const { id } = req.params;

    const locationResult = await pool.query(
      'SELECT * FROM locations WHERE id = $1 AND address IS NOT NULL',
      [id]
    );

    if (!locationResult.rows.length)
      return res.status(404).json({ message: 'Location not found' });

    const location = locationResult.rows[0];
    const programs = await getProgramsByLocationId(id);

    res.json({ ...location, programs });
  } catch (err) {
    next(err);
  }
};

export const getLocationOccupancy = async (req, res, next) => {
  try {
    const { id } = req.params;

    const locationResult = await pool.query('SELECT * FROM locations WHERE id = $1', [id]);
    if (!locationResult.rows.length)
      return res.status(404).json({ message: 'Location not found' });

    const location = locationResult.rows[0];
    const programs = await getProgramsByLocationId(id);

    res.json({ ...location, programs });
  } catch (err) {
    next(err);
  }
};

export const getLocationsForMap = async (req, res, next) => {
  try {
    // Fetch only location info first
    const query = `
      SELECT id, location_name, address, city, province,
             latitude, longitude, shelter_type, organization_name,
             last_refreshed
      FROM locations
      WHERE address IS NOT NULL
    `;
    const result = await pool.query(query);
    const locations = result.rows;

    // Filter out locations without coordinates
    const validLocations = locations.filter(
      loc => loc.latitude != null && loc.longitude != null
    );

    // Fetch programs for each location in parallel
    const locationsWithPrograms = await Promise.all(
      validLocations.map(async loc => {
        const programs = await getProgramsByLocationId(loc.id);
        return { ...loc, programs };
      })
    );

    res.json(locationsWithPrograms);
  } catch (err) {
    next(err);
  }
};