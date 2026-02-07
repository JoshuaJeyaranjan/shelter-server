import "dotenv/config";
import axios from "axios";
import pool from "../config/db";

const CKAN_URL = process.env.CKAN_URL;
const RESOURCE_ID = process.env.RESOURCE_ID;
const FETCH_LIMIT = 5000;
const BATCH_SIZE = 500;

const today = new Date().toISOString().slice(0, 10);

const toInt = (v) => (v != null && !isNaN(v) ? parseInt(v) : null);
const clean = (v) => (typeof v === "string" ? v.trim() : v) || "";

function normalizeRecord(r) {
  if (!r.LOCATION_ID) return null;
  return {
    id: toInt(r.LOCATION_ID),
    location_name: clean(r.LOCATION_NAME),
    address: clean(r.LOCATION_ADDRESS),
    city: clean(r.LOCATION_CITY),
    province: clean(r.LOCATION_PROVINCE),
    programs: [
      {
        program_name: clean(r.PROGRAM_NAME),
        sector: clean(r.SECTOR),
        overnight_service_type: clean(r.OVERNIGHT_SERVICE_TYPE),
        service_user_count: toInt(r.SERVICE_USER_COUNT),
        capacity_actual_bed: toInt(r.CAPACITY_ACTUAL_BED),
        occupied_beds: toInt(r.OCCUPIED_BEDS),
        unoccupied_beds: toInt(r.UNOCCUPIED_BEDS),
        capacity_actual_room: toInt(r.CAPACITY_ACTUAL_ROOM),
        occupied_rooms: toInt(r.OCCUPIED_ROOMS),
        unoccupied_rooms: toInt(r.UNOCCUPIED_ROOMS),
        occupancy_date: r.OCCUPANCY_DATE || null,
      },
    ],
  };
}

async function fetchAllRecords(resourceId) {
  let offset = 0;
  let allRecords = [];
  let totalCount = 0;

  do {
    const { data } = await axios.get(CKAN_URL, {
      params: { id: resourceId, limit: FETCH_LIMIT, offset },
    });

    const records = data.result.records.map(normalizeRecord).filter(Boolean);
    allRecords = allRecords.concat(records);
    totalCount = data.result.total;
    offset += FETCH_LIMIT;

    console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`);
  } while (offset < totalCount);

  return allRecords;
}

function deduplicatePrograms(locations) {
  return locations.map((loc) => {
    const programMap = new Map();
    loc.programs.forEach((p) => {
      if (!p.program_name) return;
      const key = p.program_name.toLowerCase();
      if (!programMap.has(key)) programMap.set(key, p);
      else {
        const existing = programMap.get(key);
        existing.service_user_count =
          p.service_user_count ?? existing.service_user_count;
        existing.capacity_actual_bed =
          p.capacity_actual_bed ?? existing.capacity_actual_bed;
        existing.occupied_beds = p.occupied_beds ?? existing.occupied_beds;
        existing.unoccupied_beds =
          p.unoccupied_beds ?? existing.unoccupied_beds;
        existing.capacity_actual_room =
          p.capacity_actual_room ?? existing.capacity_actual_room;
        existing.occupied_rooms = p.occupied_rooms ?? existing.occupied_rooms;
        existing.unoccupied_rooms =
          p.unoccupied_rooms ?? existing.unoccupied_rooms;
        existing.occupancy_date = p.occupancy_date ?? existing.occupancy_date;
      }
    });
    return { ...loc, programs: Array.from(programMap.values()) };
  });
}

async function insertPrograms(client, locations) {
  const batches = [];
  for (let i = 0; i < locations.length; i += BATCH_SIZE) {
    batches.push(locations.slice(i, i + BATCH_SIZE));
  }

  let totalInserted = 0;
  let totalSkipped = 0;

  for (const batch of batches) {
    const values = [];
    const placeholders = [];

    batch.forEach((loc, locIdx) => {
      const locId = loc.id;
      if (!locId) {
        console.warn(
          `⚠️ Skipping programs for unknown location: ${loc.location_name}`,
        );
        totalSkipped += loc.programs.length;
        return;
      }

      loc.programs.forEach((p, progIdx) => {
        if (!p.program_name) {
          console.warn(
            `⚠️ Skipping program with missing name at location ${locId}`,
          );
          totalSkipped++;
          return;
        }
        values.push(
          locId,
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
          p.occupancy_date,
        );
        const base = values.length - 12 + 1;
        placeholders.push(
          `($${base},$${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},NOW() AT TIME ZONE 'America/Toronto')`,
        );
        totalInserted++;
      });
    });

    if (values.length) {
      const query = `
        INSERT INTO programs (
          location_id, program_name, sector, overnight_service_type,
          service_user_count, capacity_actual_bed, occupied_beds, unoccupied_beds,
          capacity_actual_room, occupied_rooms, unoccupied_rooms, occupancy_date, last_refreshed
        )
        VALUES ${placeholders.join(",")}
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
          occupancy_date = EXCLUDED.occupancy_date,
          last_refreshed = NOW() AT TIME ZONE 'America/Toronto'
      `;
      try {
        await client.query(query, values);
        console.log(`✅ Batch inserted/updated programs: ${totalInserted}`);
      } catch (err) {
        console.error("❌ Batch insert failed:", err.message);
      }
    }
  }

  console.log("🎉 Program seeding complete!");
  console.log(`  Programs inserted/updated: ${totalInserted}`);
  console.log(`  Programs skipped: ${totalSkipped}`);
}

async function seedPrograms() {
  const client = await pool.connect();
  try {
    console.log("🌐 Fetching programs from CKAN...");
    const allRecords = await fetchAllRecords(RESOURCE_ID);
    if (!allRecords.length) return console.log("⚠️ No program records found.");

    const maxDate = allRecords
      .flatMap((r) => r.programs.map((p) => p.occupancy_date))
      .filter(Boolean)
      .sort()
      .reverse()[0];

    const latestRecords = allRecords
      .map((r) => ({
        ...r,
        programs: r.programs.filter((p) => p.occupancy_date === maxDate),
      }))
      .filter((r) => r.programs.length > 0);

    console.log(`✅ Records for latest date: ${latestRecords.length}`);

    const deduped = deduplicatePrograms(latestRecords);
    await insertPrograms(client, deduped);
  } catch (err) {
    console.error("❌ Seeding programs failed:", err);
  } finally {
    client.release();
  }
}

if (require.main === module) {
  seedPrograms().catch(console.error);
}

module.exports = { seedPrograms };
