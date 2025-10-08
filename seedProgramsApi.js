require('dotenv').config();
const pool = require('./config/db');
const { getLocationsFromDB } = require('./utils/getLocationsFromDB');

const BATCH_SIZE = 500;

/**
 * Fetch programs for seeding using the provided client
 */
async function getProgramsForSeeding(client) {
  console.log('⏳ Running query to fetch programs for seeding...');
  const startTime = Date.now();

  const res = await client.query(`
    SELECT
      l.id AS location_id,
      l.location_name,
      l.address,
      l.city,
      l.province,
      l.latitude,
      l.longitude,
      p.id AS program_id,
      p.program_name,
      p.sector,
      p.overnight_service_type,
      p.service_user_count,
      p.capacity_actual_bed,
      p.occupied_beds,
      p.unoccupied_beds,
      p.capacity_actual_room,
      p.occupied_rooms,
      p.unoccupied_rooms,
      p.occupancy_date
    FROM locations l
    LEFT JOIN programs p ON p.location_id = l.id
    ORDER BY l.id, p.id
  `);

  console.log(`✅ Query complete. Fetched ${res.rows.length} rows in ${Date.now() - startTime}ms`);

  const locationMap = new Map();

  res.rows.forEach((row, index) => {
    if (index > 0 && index % 50 === 0) console.log(`  ⬆ Processed ${index} rows...`);

    const locKey = row.location_id;

    if (!locationMap.has(locKey)) {
      locationMap.set(locKey, {
        id: row.location_id,
        location_name: row.location_name,
        address: row.address,
        city: row.city,
        province: row.province,
        latitude: row.latitude,
        longitude: row.longitude,
        programs: [],
      });
    }

    if (row.program_id) {
      locationMap.get(locKey).programs.push({
        id: row.program_id,
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
        occupancy_date: row.occupancy_date,
      });
    }
  });

  console.log(`✅ Finished normalizing locations. Total locations: ${locationMap.size}`);
  return Array.from(locationMap.values());
}

/**
 * Inserts or updates programs in batches
 */
async function insertPrograms(programs, idMap, client) {
  let totalInserted = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;

  for (const loc of programs) {
    const locKey = `${loc.location_name}||${loc.address}||${loc.city}||${loc.province}`;
    const locId = idMap.get(locKey);

    if (!locId) {
      totalSkipped++;
      continue;
    }

    const programMap = new Map();
    loc.programs.forEach(p => {
      if (!p.program_name) return totalSkipped++;
      const key = p.program_name.trim();
      if (!programMap.has(key)) programMap.set(key, p);
      else {
        const existing = programMap.get(key);
        existing.service_user_count = p.service_user_count ?? existing.service_user_count;
        existing.capacity_actual_bed = p.capacity_actual_bed ?? existing.capacity_actual_bed;
        existing.occupied_beds = p.occupied_beds ?? existing.occupied_beds;
        existing.unoccupied_beds = p.unoccupied_beds ?? existing.unoccupied_beds;
        existing.capacity_actual_room = p.capacity_actual_room ?? existing.capacity_actual_room;
        existing.occupied_rooms = p.occupied_rooms ?? existing.occupied_rooms;
        existing.unoccupied_rooms = p.unoccupied_rooms ?? existing.unoccupied_rooms;
        existing.occupancy_date = p.occupancy_date ?? existing.occupancy_date;
      }
    });

    const programArray = Array.from(programMap.values());

    for (let i = 0; i < programArray.length; i += BATCH_SIZE) {
      const batch = programArray.slice(i, i + BATCH_SIZE);

      const placeholders = batch
        .map((_, j) => `(${Array.from({ length: 12 }, (_, k) => `$${j * 12 + k + 1}`).join(',')})`)
        .join(',');

      const values = batch.flatMap(p => [
        p.program_name?.trim() || null,
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

      const res = await client.query(queryText, values);
      res.rows.forEach(r => (r.xmax === '0' ? totalInserted++ : totalUpdated++));
    }
  }

  console.log('🎉 Program seeding complete!');
  console.log(`  Programs inserted: ${totalInserted}`);
  console.log(`  Programs updated: ${totalUpdated}`);
  console.log(`  Programs skipped: ${totalSkipped}`);
}

/**
 * Main function
 */
async function seedProgramsFromDB() {
  const client = await pool.connect();
  try {
    console.log('✅ Fetching locations from DB...');
    const idMap = await getLocationsFromDB(client); // make sure it uses client
    console.log(`✅ Fetched ${idMap.size} locations from DB`);

    console.log('🚀 About to fetch programs...');
    const programs = await getProgramsForSeeding(client);
    console.log(`✅ Programs fetched: ${programs.length} locations`);

    console.log('🔄 Inserting/updating programs...');
    await insertPrograms(programs, idMap, client);
  } catch (err) {
    console.error('❌ Seeding failed:', err);
  } finally {
    client.release();
    console.log('💤 Ending pool...');
    await pool.end();
  }
}

if (require.main === module) {
  seedProgramsFromDB();
}

module.exports = { insertPrograms, seedProgramsFromDB };