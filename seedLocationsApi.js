require('dotenv').config();
const axios = require('axios');
const pool = require('./config/db');

const PACKAGE_ID = '21c83b32-d5a8-4106-a54f-010dbe49f6f2';
const FETCH_LIMIT = 5000;

// Normalize CKAN record
function normalizeRecord(r) {
  if (!r._id) return null;
  return {
    id: r._id,
    shelter_id: r.SHELTER_ID || null,
    organization_name: r.ORGANIZATION_NAME || null,
    shelter_group: r.SHELTER_GROUP || null,
    location_name: r.LOCATION_NAME?.trim() || null,
    address: r.LOCATION_ADDRESS?.trim() || null,
    postal_code: r.LOCATION_POSTAL_CODE?.trim() || null,
    city: r.LOCATION_CITY?.trim() || null,
    province: r.LOCATION_PROVINCE?.trim() || null,
    latitude: r.LATITUDE ? parseFloat(r.LATITUDE) : null,
    longitude: r.LONGITUDE ? parseFloat(r.LONGITUDE) : null,
    programs: [{
      id: r._id,
      program_name: r.PROGRAM_NAME?.trim() || null,
      sector: r.SECTOR?.trim() || null,
      overnight_service_type: r.OVERNIGHT_SERVICE_TYPE?.trim() || null,
      service_user_count: r.SERVICE_USER_COUNT ? parseInt(r.SERVICE_USER_COUNT) : null,
      capacity_actual_bed: r.CAPACITY_ACTUAL_BED ? parseInt(r.CAPACITY_ACTUAL_BED) : null,
      capacity_actual_room: r.CAPACITY_ACTUAL_ROOM ? parseInt(r.CAPACITY_ACTUAL_ROOM) : null,
      occupied_beds: r.OCCUPIED_BEDS ? parseInt(r.OCCUPIED_BEDS) : null,
      unoccupied_beds: r.UNOCCUPIED_BEDS ? parseInt(r.UNOCCUPIED_BEDS) : null,
      occupied_rooms: r.OCCUPIED_ROOMS ? parseInt(r.OCCUPIED_ROOMS) : null,
      unoccupied_rooms: r.UNOCCUPIED_ROOMS ? parseInt(r.UNOCCUPIED_ROOMS) : null,
      occupancy_date: r.OCCUPANCY_DATE || null
    }]
  };
}

// Deduplicate locations by name + address + city + province
function deduplicateLocations(records) {
  const map = new Map();
  for (const r of records) {
    if (!r.location_name || !r.address) continue;
    const key = `${r.location_name}||${r.address}||${r.city || ''}||${r.province || ''}`;
    if (!map.has(key)) map.set(key, { ...r, programs: [...r.programs] });
    else map.get(key).programs.push(...r.programs);
  }
  return Array.from(map.values());
}

// Fetch all CKAN records with pagination
async function fetchAllRecords(resourceId) {
  let offset = 0;
  let allRecords = [];
  let totalCount = 0;

  do {
    const { data } = await axios.get(
      'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search',
      { params: { id: resourceId, limit: FETCH_LIMIT, offset } }
    );

    const records = data.result.records.map(normalizeRecord).filter(Boolean);
    allRecords = allRecords.concat(records);
    totalCount = data.result.total;
    offset += FETCH_LIMIT;
    console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`);
  } while (offset < totalCount);

  return allRecords;
}

// Insert/update locations without overwriting existing lat/lng if null
async function insertLocations(client, locations) {
  const idMap = new Map();
  let processed = 0;

  for (const loc of locations) {
    try {
      const res = await client.query(
        `INSERT INTO locations (location_name, address, postal_code, city, province, latitude, longitude)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (location_name, address, city, province) DO UPDATE
           SET latitude  = COALESCE(EXCLUDED.latitude, locations.latitude),
               longitude = COALESCE(EXCLUDED.longitude, locations.longitude)
         RETURNING id`,
        [
          loc.location_name,
          loc.address,
          loc.postal_code,
          loc.city,
          loc.province,
          loc.latitude,
          loc.longitude,
        ]
      );

      idMap.set(`${loc.location_name}||${loc.address}||${loc.city || ''}||${loc.province || ''}`, res.rows[0].id);
      processed++;
    } catch (err) {
      console.error('❌ Error inserting location:', loc.location_name, err);
    }
  }

  console.log(`✅ Locations processed: ${processed} / ${locations.length}`);
  return idMap;
}

// Main seeding function
async function seedLocations() {
  const client = await pool.connect();
  try {
    console.log('🌐 Connecting to database...');

    const { data: pkgData } = await axios.get(
      `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${PACKAGE_ID}`
    );

    const resources = pkgData.result.resources.filter(r => r.datastore_active);
    if (!resources.length) throw new Error('No active datastore resources found');

    const resourceId = resources[0].id;
    const allRecords = await fetchAllRecords(resourceId);

    console.log(`📦 Total fetched: ${allRecords.length}`);
    const locations = deduplicateLocations(allRecords);
    console.log(`✅ Total locations after deduplication: ${locations.length}`);

    const idMap = await insertLocations(client, locations);

    // Update metadata
    await client.query(`
      INSERT INTO shelter_metadata (id, last_refreshed)
      VALUES (1, NOW())
      ON CONFLICT (id) DO UPDATE SET last_refreshed = EXCLUDED.last_refreshed
    `);

    console.log('🎉 Location seeding complete!');
    return idMap;
  } catch (err) {
    console.error('❌ Error seeding locations:', err);
    throw err;
  } finally {
    client.release();
  }
}

// Run standalone
if (require.main === module) {
  seedLocations().catch(err => console.error(err));
}

module.exports = { seedLocations }