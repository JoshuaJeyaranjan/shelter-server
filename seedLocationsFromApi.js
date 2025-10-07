require('dotenv').config();
const axios = require('axios');
const pool = require('./config/db');

const PACKAGE_ID = '21c83b32-d5a8-4106-a54f-010dbe49f6f2';
const BATCH_SIZE = 500;

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

function deduplicateLocations(records) {
  const locationMap = new Map();

  for (const r of records) {
    if (!r.location_name || !r.address) continue;

    const key = `${r.location_name.trim()}||${r.address.trim()}||${r.city?.trim()}||${r.province?.trim()}`;

    if (!locationMap.has(key)) {
      locationMap.set(key, {
        ...r,
        location_name: r.location_name.trim(),
        address: r.address.trim(),
        city: r.city?.trim(),
        province: r.province?.trim(),
        programs: [...r.programs]
      });
    } else {
      locationMap.get(key).programs.push(...r.programs);
    }
  }

  return Array.from(locationMap.values());
}

async function fetchAllRecords(resourceId) {
  const limit = 5000;
  let offset = 0;
  let allRecords = [];
  let totalCount = 0;

  do {
    const { data } = await axios.get(
      'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search',
      { params: { id: resourceId, limit, offset } }
    );
    const records = data.result.records.map(normalizeRecord).filter(Boolean);
    allRecords = allRecords.concat(records);
    totalCount = data.result.total;
    offset += limit;
    console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`);
  } while (offset < totalCount);

  return allRecords;
}

async function insertLocations(client, locations) {
  if (!locations.length) return { idMap: new Map(), inserted: 0, alreadyExists: 0 };

  let inserted = 0;
  let alreadyExists = 0;
  const idMap = new Map();

  for (const l of locations) {
    const { location_name, address, postal_code, city, province, latitude, longitude } = l;

    // Check if the location already exists
    const existing = await client.query(
      `SELECT id, latitude, longitude FROM locations 
       WHERE location_name = $1 AND address = $2 AND city = $3 AND province = $4`,
      [location_name, address, city, province]
    );

    if (existing.rows.length > 0) {
      const existingRow = existing.rows[0];
      alreadyExists++;
      idMap.set(`${location_name}||${address}||${city}||${province}`, existingRow.id);

      // Optionally update non-destructive fields (e.g., city casing or postal code changes)
      await client.query(`
        UPDATE locations
        SET postal_code = $1,
            city = $2,
            province = $3
        WHERE id = $4
      `, [postal_code, city, province, existingRow.id]);

    } else {
      // Insert new location, respecting lat/long if available
      const res = await client.query(`
        INSERT INTO locations (location_name, address, postal_code, city, province, latitude, longitude)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id
      `, [location_name, address, postal_code, city, province, latitude, longitude]);

      inserted++;
      idMap.set(`${location_name}||${address}||${city}||${province}`, res.rows[0].id);
    }
  }

  return { idMap, inserted, alreadyExists };
}

async function insertPrograms(client, locations, idMap) {
  let totalInserted = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;

  // Use a global map to ensure no duplicates across batches
  const seenKeys = new Set();
  const programs = [];

  for (const loc of locations) {
    const locKey = `${loc.location_name}||${loc.address}||${loc.city}||${loc.province}`;
    const locId = idMap.get(locKey);
    if (!locId) continue;

    const programMap = new Map();

    for (const p of loc.programs) {
      if (!p.program_name) {
        totalSkipped++;
        continue;
      }
      const programName = p.program_name.trim();
      const key = `${locId}||${programName}`;

      if (programMap.has(key)) {
        const existing = programMap.get(key);
        existing.service_user_count = p.service_user_count ?? existing.service_user_count;
        existing.capacity_actual_bed = p.capacity_actual_bed ?? existing.capacity_actual_bed;
        existing.occupied_beds = p.occupied_beds ?? existing.occupied_beds;
        existing.unoccupied_beds = p.unoccupied_beds ?? existing.unoccupied_beds;
        existing.capacity_actual_room = p.capacity_actual_room ?? existing.capacity_actual_room;
        existing.occupied_rooms = p.occupied_rooms ?? existing.occupied_rooms;
        existing.unoccupied_rooms = p.unoccupied_rooms ?? existing.unoccupied_rooms;
        existing.occupancy_date = p.occupancy_date ?? existing.occupancy_date;
      } else {
        programMap.set(key, { ...p, program_name: programName });
      }
    }

    for (const [key, p] of programMap.entries()) {
      // Skip if already added globally
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      programs.push([
        p.program_name,
        p.sector?.trim() || null,
        p.overnight_service_type?.trim() || null,
        p.service_user_count ?? null,
        p.capacity_actual_bed ?? null,
        p.occupied_beds ?? null,
        p.unoccupied_beds ?? null,
        p.capacity_actual_room ?? null,
        p.occupied_rooms ?? null,
        p.unoccupied_rooms ?? null,
        p.occupancy_date || null,
        locId
      ]);
    }
  }

  const BATCH_SIZE = 500;
  for (let i = 0; i < programs.length; i += BATCH_SIZE) {
    const batch = programs.slice(i, i + BATCH_SIZE);
    const placeholders = batch
      .map((_, j) => `(${Array.from({ length: 12 }, (_, k) => `$${j * 12 + k + 1}`).join(',')})`)
      .join(',');

    const queryText = `
      INSERT INTO programs (
        program_name, sector, overnight_service_type, service_user_count,
        capacity_actual_bed, occupied_beds, unoccupied_beds,
        capacity_actual_room, occupied_rooms, unoccupied_rooms,
        occupancy_date, location_id
      ) VALUES ${placeholders}
      ON CONFLICT (location_id, program_name) DO UPDATE SET
        sector = EXCLUDED.sector,
        overnight_service_type = EXCLUDED.overnight_service_type,
        service_user_count = EXCLUDED.service_user_count,
        capacity_actual_bed = EXCLUDED.capacity_actual_bed,
        occupied_beds = EXCLUDED.occupied_beds,
        unoccupied_beds = EXCLUDED.unoccupied_beds,
        capacity_actual_room = EXCLUDED.capacity_actual_room,
        occupied_rooms = EXCLUDED.occupied_rooms,
        unoccupied_rooms = EXCLUDED.unoccupied_rooms,
        occupancy_date = EXCLUDED.occupancy_date
      RETURNING xmax;
    `;

    const values = batch.flat();
    const res = await client.query(queryText, values);
    res.rows.forEach(r => (r.xmax === '0' ? totalInserted++ : totalUpdated++));
  }

  return { totalInserted, totalUpdated, totalSkipped };
}

async function seedLocationsFromAPI() {
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

    const { idMap, inserted, alreadyExists } = await insertLocations(client, locations);
    const { totalInserted, totalUpdated, totalSkipped } = await insertPrograms(client, locations, idMap);

    await client.query(`
      INSERT INTO shelter_metadata (id, last_refreshed)
      VALUES (1, NOW())
      ON CONFLICT (id) DO UPDATE SET last_refreshed = EXCLUDED.last_refreshed;
    `);

    console.log('🎉 Seeding complete! Summary:');
    console.log(`  Locations inserted: ${inserted}`);
    console.log(`  Locations already existed: ${alreadyExists}`);
    console.log(`  Programs inserted: ${totalInserted}`);
    console.log(`  Programs updated: ${totalUpdated}`);
    console.log(`  Programs skipped (missing name): ${totalSkipped}`);

  } catch (err) {
    console.error('❌ Error seeding locations:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

module.exports = { seedLocationsFromAPI };

if (require.main === module) {
  seedLocationsFromAPI();
}