require("dotenv").config();
const axios = require("axios");
const pool = require("../config/db");

const RESOURCE_ID = "42714176-4f05-44e6-b157-2b57f29b856a";
const FETCH_LIMIT = 5000;

function normalizeRecord(r) {
  if (!r.LOCATION_ID) return null;
  const toInt = (val) => (val != null && !isNaN(val) ? parseInt(val) : null);
  const clean = (val) => (typeof val === "string" ? val.trim() : val) || "";

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

async function insertLocations(client, locations) {
  let processed = 0;

  for (const loc of locations) {
    try {
      await client.query(
        `
INSERT INTO locations (
  id, organization_id, organization_name, shelter_id, shelter_group,
  location_name, address, postal_code, city, province,
  shelter_type, population_served, program_model, sector, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
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
  created_at = EXCLUDED.created_at
        `,
        [
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
        ],
      );
      await client.query(
        `UPDATE locations
         SET last_refreshed = NOW() AT TIME ZONE 'America/Toronto'
         WHERE id = $1`,
        [loc.id],
      );
      processed++;
    } catch (err) {
      console.error(
        "❌ Error inserting location:",
        loc.location_name,
        err.message,
      );
    }
  }

  console.log(`✅ Locations processed: ${processed} / ${locations.length}`);
}

async function seedLocations() {
  const client = await pool.connect();
  try {
    console.log("🌐 Connecting to database...");

    const allRecords = await fetchAllRecords(RESOURCE_ID);

    console.log(`📦 Total fetched: ${allRecords.length}`);

    if (allRecords[0]?.sector) {
      console.log("🧠 Sample sector value:", allRecords[0].sector);
    }

    const locations = allRecords;
    console.log(`✅ Locations ready to insert: ${locations.length}`);

    await insertLocations(client, locations);

    console.log("🎉 Location seeding complete!");
  } catch (err) {
    console.error("❌ Error seeding locations:", err);
  } finally {
    client.release();
  }
}

if (require.main === module) {
  seedLocations().catch((err) => console.error(err));
}

module.exports = { seedLocations };
