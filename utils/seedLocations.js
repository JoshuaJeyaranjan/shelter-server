import "dotenv/config";
import axios from "axios";
import pool from "../config/db";

const CKAN_URL = process.env.CKAN_URL; 
const RESOURCE_ID = process.env.RESOURCE_ID;
const FETCH_LIMIT = 5000;
const BATCH_SIZE = 500; 

const toInt = (val) => (val != null && !isNaN(val) ? parseInt(val) : null);
const clean = (val) => (typeof val === "string" ? val.trim() : val) || "";

function normalizeRecord(r) {
  if (!r.LOCATION_ID) return null;
  return {
    id: toInt(r.LOCATION_ID),
    organization_id: toInt(r.ORGANIZATION_ID),
    organization_name: clean(r.ORGANIZATION_NAME),
    shelter_id: toInt(r.SHELTER_ID),
    shelter_group: clean(r.SHELTER_GROUP),
    location_name: clean(r.LOCATION_NAME),
    address: clean(r.LOCATION_ADDRESS),
    postal_code: clean(r.LOCATION_POSTAL_CODE),
    city: clean(r.LOCATION_CITY),
    province: clean(r.LOCATION_PROVINCE),
    shelter_type: clean(r.PROGRAM_MODEL),
    population_served: clean(r.PROGRAM_AREA),
    program_model: clean(r.PROGRAM_MODEL),
    sector: clean(r.SECTOR),
    created_at: new Date(),
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

async function insertLocations(client, locations) {
  const insertQuery = `
    INSERT INTO locations (
      id, organization_id, organization_name, shelter_id, shelter_group,
      location_name, address, postal_code, city, province,
      shelter_type, population_served, program_model, sector, created_at, last_refreshed
    )
    VALUES %L
    ON CONFLICT (id) DO UPDATE SET
      organization_id = EXCLUDED.organization_id,
      organization_name = EXCLUDED.organization_name,
      shelter_id = EXCLUDED.shelter_id,
      shelter_group = EXCLUDED.shelter_group,
      location_name = EXCLUDED.location_name,
      address = EXCLUDED.address,
      postal_code = EXCLUDED.postal_code,
      city = EXCLUDED.city,
      province = EXCLUDED.province,
      shelter_type = EXCLUDED.shelter_type,
      population_served = EXCLUDED.population_served,
      program_model = EXCLUDED.program_model,
      sector = EXCLUDED.sector,
      created_at = EXCLUDED.created_at,
      last_refreshed = NOW() AT TIME ZONE 'America/Toronto'
  `;

  const batches = [];
  for (let i = 0; i < locations.length; i += BATCH_SIZE) {
    batches.push(locations.slice(i, i + BATCH_SIZE));
  }

  let processed = 0;
  for (const batch of batches) {
    const values = batch
      .map((loc) => [
        loc.id,
        loc.organization_id,
        loc.organization_name,
        loc.shelter_id,
        loc.shelter_group,
        loc.location_name,
        loc.address,
        loc.postal_code,
        loc.city,
        loc.province,
        loc.shelter_type,
        loc.population_served,
        loc.program_model,
        loc.sector,
        loc.created_at,
      ])
      .map((row) =>
        `(${row.map((_, idx) => `$${idx + 1}`).join(",")}, NOW() AT TIME ZONE 'America/Toronto')`
      );

    const flatValues = batch.flatMap((loc) => [
      loc.id,
      loc.organization_id,
      loc.organization_name,
      loc.shelter_id,
      loc.shelter_group,
      loc.location_name,
      loc.address,
      loc.postal_code,
      loc.city,
      loc.province,
      loc.shelter_type,
      loc.population_served,
      loc.program_model,
      loc.sector,
      loc.created_at,
    ]);

    try {
      await client.query(insertQuery.replace("%L", values.join(",")), flatValues);
      processed += batch.length;
      console.log(`✅ Inserted/updated batch: ${processed}/${locations.length}`);
    } catch (err) {
      console.error("❌ Batch insert failed:", err.message);
    }
  }
}

async function seedLocations() {
  const client = await pool.connect();
  try {
    console.log("🌐 Connecting to database...");
    const allRecords = await fetchAllRecords(RESOURCE_ID);
    if (!allRecords.length) return console.log("⚠️ No records found.");

    console.log(`📦 Total fetched: ${allRecords.length}`);
    await insertLocations(client, allRecords);

    console.log("🎉 Location seeding complete!");
  } catch (err) {
    console.error("❌ Error seeding locations:", err);
  } finally {
    client.release();
  }
}

if (require.main === module) {
  seedLocations().catch(console.error);
}

module.exports = { seedLocations };