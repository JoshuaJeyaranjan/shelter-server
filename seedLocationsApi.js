require("dotenv").config();
const axios = require("axios");
const pool = require("./config/db");

const PACKAGE_ID = "21c83b32-d5a8-4106-a54f-010dbe49f6f2";
const FETCH_LIMIT = 5000;

/**
 * Normalize a CKAN record safely
 */
function normalizeRecord(r) {
  if (!r._id) return null;

  // Normalize strings and convert numbers
  const toInt = (val) => (val != null && !isNaN(val) ? parseInt(val) : null);
  const toFloat = (val) => (val != null && !isNaN(val) ? parseFloat(val) : null);
  const clean = (val) => (typeof val === "string" ? val.trim() : val) || "";

  return {
    id: r._id,
    shelter_id: clean(r.SHELTER_ID),
    organization_name: clean(r.ORGANIZATION_NAME),
    shelter_group: clean(r.SHELTER_GROUP),
    location_name: clean(r.LOCATION_NAME),
    address: clean(r.LOCATION_ADDRESS),
    postal_code: clean(r.LOCATION_POSTAL_CODE),
    city: clean(r.LOCATION_CITY),
    province: clean(r.LOCATION_PROVINCE),
    latitude: toFloat(r.LATITUDE),
    longitude: toFloat(r.LONGITUDE),
    programs: [
      {
        id: r._id,
        program_name: clean(r.PROGRAM_NAME),
        sector: clean(r.SECTOR),
        overnight_service_type: clean(r.OVERNIGHT_SERVICE_TYPE),
        service_user_count: toInt(r.SERVICE_USER_COUNT),
        capacity_actual_bed: toInt(r.CAPACITY_ACTUAL_BED),
        capacity_actual_room: toInt(r.CAPACITY_ACTUAL_ROOM),
        occupied_beds: toInt(r.OCCUPIED_BEDS),
        unoccupied_beds: toInt(r.UNOCCUPIED_BEDS),
        occupied_rooms: toInt(r.OCCUPIED_ROOMS),
        unoccupied_rooms: toInt(r.UNOCCUPIED_ROOMS),
        occupancy_date: r.OCCUPANCY_DATE || null,
      },
    ],
  };
}

/**
 * Deduplicate by location_name + address (case-insensitive),
 * and merge city/province/postal_code safely.
 */
function deduplicateLocations(records) {
  const map = new Map();

  for (const r of records) {
    if (!r.location_name || !r.address) continue;

    const key = `${r.location_name.toLowerCase()}||${r.address.toLowerCase()}`;

    if (!map.has(key)) {
      map.set(key, { ...r });
    } else {
      const existing = map.get(key);

      // Preserve any missing but now-available fields
      existing.city = existing.city || r.city;
      existing.province = existing.province || r.province;
      existing.postal_code = existing.postal_code || r.postal_code;

      // Don’t overwrite lat/lon if we already have valid ones
      existing.latitude = existing.latitude ?? r.latitude;
      existing.longitude = existing.longitude ?? r.longitude;

      // Merge programs
      existing.programs.push(...r.programs);
    }
  }

  return Array.from(map.values());
}

/**
 * Fetch CKAN data with pagination
 */
async function fetchAllRecords(resourceId) {
  let offset = 0;
  let allRecords = [];
  let totalCount = 0;

  do {
    const { data } = await axios.get(
      "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search",
      {
        params: { id: resourceId, limit: FETCH_LIMIT, offset },
      }
    );

    const records = data.result.records.map(normalizeRecord).filter(Boolean);
    allRecords = allRecords.concat(records);
    totalCount = data.result.total;
    offset += FETCH_LIMIT;

    console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`);
  } while (offset < totalCount);

  return allRecords;
}

/**
 * Insert or update locations safely
 */
async function insertLocations(client, locations) {
  const idMap = new Map();
  let processed = 0;

  for (const loc of locations) {
    try {
      // Attempt insert first
const res = await client.query(
  `
  INSERT INTO locations (location_name, address, postal_code, city, province, latitude, longitude)
  VALUES ($1,$2,$3,$4,$5,$6,$7)
  ON CONFLICT (
    UPPER(TRIM(location_name)),
    UPPER(TRIM(address)),
    UPPER(TRIM(city)),
    UPPER(TRIM(province))
  )
  DO UPDATE SET
    postal_code = COALESCE(locations.postal_code, EXCLUDED.postal_code),
    city = COALESCE(locations.city, EXCLUDED.city),
    province = COALESCE(locations.province, EXCLUDED.province),
    latitude = COALESCE(locations.latitude, EXCLUDED.latitude),
    longitude = COALESCE(locations.longitude, EXCLUDED.longitude)
  RETURNING id
  `,
  [
    loc.location_name,
    loc.address,
    loc.postal_code || null,
    loc.city || null,
    loc.province || null,
    loc.latitude,
    loc.longitude,
  ]
);
      // If insert succeeded, use returned id
      if (res.rows[0]) {
        idMap.set(
          `${loc.location_name}||${loc.address}||${loc.city || ""}||${loc.province || ""}`,
          res.rows[0].id
        );
      } else {
        // Conflict happened; fetch existing id
        const existing = await client.query(
          `
          SELECT id FROM locations
          WHERE UPPER(TRIM(location_name)) = UPPER(TRIM($1))
            AND UPPER(TRIM(address)) = UPPER(TRIM($2))
            AND UPPER(TRIM(city)) = UPPER(TRIM($3))
            AND UPPER(TRIM(province)) = UPPER(TRIM($4))
          `,
          [
            loc.location_name,
            loc.address,
            loc.city || "Unknown",
            loc.province || "Unknown",
          ]
        );
        if (existing.rows[0]) {
          idMap.set(
            `${loc.location_name}||${loc.address}||${loc.city || ""}||${loc.province || ""}`,
            existing.rows[0].id
          );
        } else {
          console.warn(
            "⚠️ Could not find or insert location:",
            loc.location_name
          );
        }
      }

      processed++;
    } catch (err) {
      console.error("❌ Error inserting location:", loc.location_name, err.message);
    }
  }

  console.log(`✅ Locations processed: ${processed} / ${locations.length}`);
  return idMap;
}
/**
 * Main seeding entry point
 */
async function seedLocations() {
  const client = await pool.connect();
  try {
    console.log("🌐 Connecting to database...");

    const { data: pkgData } = await axios.get(
      `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${PACKAGE_ID}`
    );

    const resources = pkgData.result.resources.filter((r) => r.datastore_active);
    if (!resources.length) throw new Error("No active datastore resources found");

    const resourceId = resources[0].id;
    const allRecords = await fetchAllRecords(resourceId);

    console.log(`📦 Total fetched: ${allRecords.length}`);
    const locations = deduplicateLocations(allRecords);
    console.log(`✅ After deduplication: ${locations.length} unique locations`);

    const idMap = await insertLocations(client, locations);

    // Metadata update
    await client.query(`
      INSERT INTO shelter_metadata (id, last_refreshed)
      VALUES (1, NOW())
      ON CONFLICT (id) DO UPDATE SET last_refreshed = EXCLUDED.last_refreshed
    `);

    console.log("🎉 Location seeding complete!");
    return idMap;
  } catch (err) {
    console.error("❌ Error seeding locations:", err);
    throw err;
  } finally {
    client.release();
  }
}

// Run standalone
if (require.main === module) {
  seedLocations().catch((err) => console.error(err));
}

module.exports = { seedLocations };