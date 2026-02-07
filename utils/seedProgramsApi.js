require("dotenv").config();
const axios = require("axios");
const pool = require("../config/db");

const RESOURCE_ID = "42714176-4f05-44e6-b157-2b57f29b856a";
const FETCH_LIMIT = 5000;

const today = new Date().toISOString().slice(0, 10);

function normalizeRecord(r) {
  if (!r.LOCATION_ID) return null;
  const toInt = (v) => (v != null && !isNaN(v) ? parseInt(v) : null);
  const toFloat = (v) => (v != null && !isNaN(v) ? parseFloat(v) : null);
  const clean = (v) => (typeof v === "string" ? v.trim() : v) || "";

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
    const { data } = await axios.get(
      "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search",
      { params: { id: resourceId, limit: FETCH_LIMIT, offset } },
    );

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
        // merge numeric fields if needed
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

async function insertPrograms(programs, client) {
  let totalInserted = 0;
  let totalSkipped = 0;

  for (const loc of programs) {
    const locId = loc.id;

    if (!locId) {
      console.warn(
        `⚠️ Skipping programs for unknown location: ${loc.location_name}`,
      );
      totalSkipped += loc.programs.length;
      continue;
    }

    for (const p of loc.programs) {
      if (!p.program_name) {
        console.warn(
          `⚠️ Skipping program with missing name at location ${locId}`,
        );
        totalSkipped++;
        continue;
      }

      try {
        await client.query(
          `
          INSERT INTO programs (
            location_id, program_name, sector, overnight_service_type,
            service_user_count, capacity_actual_bed, occupied_beds, unoccupied_beds,
            capacity_actual_room, occupied_rooms, unoccupied_rooms, occupancy_date, last_refreshed
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW() AT TIME ZONE 'America/Toronto')
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
          `,
          [
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
          ],
        );

        totalInserted++;
      } catch (err) {
        console.error(
          `❌ Error inserting program ${p.program_name} at location ${locId}:`,
          err.message,
        );
        totalSkipped++;
      }
    }
  }

  console.log("🎉 Program seeding complete!");
  console.log(`  Programs inserted/updated: ${totalInserted}`);
  console.log(`  Programs skipped: ${totalSkipped}`);
}

async function seedProgramsFromAPI() {
  const client = await pool.connect();
  try {
    console.log("🌐 Fetching programs from CKAN...");

    const allRecords = await fetchAllRecords(RESOURCE_ID);

    if (!allRecords.length) {
      console.log("⚠️ No program records found in CKAN.");
      return;
    }

    const maxDate = allRecords
      .flatMap((r) => r.programs.map((p) => p.occupancy_date))
      .filter(Boolean)
      .sort()
      .reverse()[0];

    console.log(`📅 Latest occupancy date: ${maxDate}`);

    const allDates = [
      ...new Set(
        allRecords.flatMap((r) =>
          r.programs.map((p) => p.occupancy_date).filter(Boolean),
        ),
      ),
    ].sort();
    console.log("📆 Today (UTC):", today);
    console.log("📆 Latest CKAN date:", maxDate);

    if (maxDate < today) {
      console.warn(
        "⚠️ CKAN datastore appears behind CSV. Latest date is older than today.",
      );
    }

    console.log("🧪 Unique occupancy dates returned by CKAN datastore:");
    console.log(allDates.slice(-10));

    const latestRecords = allRecords
      .map((r) => ({
        ...r,
        programs: r.programs.filter((p) => p.occupancy_date === maxDate),
      }))
      .filter((r) => r.programs.length > 0);

    console.log(`✅ Records for latest date: ${latestRecords.length}`);

    const deduped = deduplicatePrograms(latestRecords);

    await insertPrograms(deduped, client);
  } catch (err) {
    console.error("❌ Seeding programs failed:", err);
  } finally {
    client.release();
    await pool.end();
    console.log("💤 Pool closed.");
  }
}

if (require.main === module) {
  seedProgramsFromAPI().catch(console.error);
}

module.exports = { seedProgramsFromAPI };
