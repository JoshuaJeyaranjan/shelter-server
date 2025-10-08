require('dotenv').config();
const pool = require('../config/db');

/**
 * Fetch all locations from the database and return a Map keyed by:
 * "location_name||address||city||province" => location_id
 */
async function getLocationsFromDB() {
  const idMap = new Map();

  try {
    const res = await pool.query(`
      SELECT id, location_name, address, city, province
      FROM locations
    `);

    res.rows.forEach((row) => {
      if (!row.location_name || !row.address) return;
      const key = `${row.location_name}||${row.address}||${row.city || ''}||${row.province || ''}`;
      idMap.set(key, row.id);
    });

    console.log(`✅ Fetched ${idMap.size} locations from DB.`);
    return idMap;
  } catch (err) {
    console.error('❌ Error fetching locations from DB:', err);
    return idMap;
  }
}

// If run directly, fetch and log
if (require.main === module) {
  (async () => {
    const map = await getLocationsFromDB();
    console.log(map);
  })();
}

module.exports = { getLocationsFromDB };