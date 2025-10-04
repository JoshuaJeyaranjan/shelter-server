// seedShelters.js
require("dotenv").config();
const fs = require("fs");
const pool = require("./config/db"); // your existing Postgres pool

async function seedShelters() {
  try {
    // Read JSON file
    const data = fs.readFileSync("./data/shelters.json", "utf-8");
    const shelters = JSON.parse(data);

    if (!Array.isArray(shelters) || shelters.length === 0) {
      console.error("Shelters JSON is empty or invalid!");
      process.exit(1);
    }

    // Truncate table to allow safe reseeding
    await pool.query("TRUNCATE TABLE shelters RESTART IDENTITY CASCADE");
    console.log(`Truncated shelters table. Ready to insert ${shelters.length} records.`);

    // Prepare insert query
    const insertQuery = `
      INSERT INTO shelters(
        organization_name, shelter_group, location_name, address,
        postal_code, city, province, program_name, sector,
        overnight_service_type, service_user_count,
        capacity_actual_bed, capacity_actual_room,
        occupied_beds, unoccupied_beds, occupied_rooms, unoccupied_rooms,
        occupancy_date, last_updated
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
      )
    `;

    let insertedCount = 0;

    for (const s of shelters) {
      await pool.query(insertQuery, [
        s.ORGANIZATION_NAME || null,
        s.SHELTER_GROUP || null,
        s.LOCATION_NAME || null,
        s.LOCATION_ADDRESS || null,
        s.LOCATION_POSTAL_CODE || null,
        s.LOCATION_CITY || null,
        s.LOCATION_PROVINCE || null,
        s.PROGRAM_NAME || null,
        s.SECTOR || null,
        s.OVERNIGHT_SERVICE_TYPE || null,
        s.SERVICE_USER_COUNT ? Number(s.SERVICE_USER_COUNT) : null,
        s.CAPACITY_ACTUAL_BED ? Number(s.CAPACITY_ACTUAL_BED) : null,
        s.CAPACITY_ACTUAL_ROOM ? Number(s.CAPACITY_ACTUAL_ROOM) : null,
        s.OCCUPIED_BEDS ? Number(s.OCCUPIED_BEDS) : null,
        s.UNOCCUPIED_BEDS ? Number(s.UNOCCUPIED_BEDS) : null,
        s.OCCUPIED_ROOMS ? Number(s.OCCUPIED_ROOMS) : null,
        s.UNOCCUPIED_ROOMS ? Number(s.UNOCCUPIED_ROOMS) : null,
        s.OCCUPANCY_DATE || null,
        new Date()
      ]);

      insertedCount++;
      if (insertedCount % 50 === 0) console.log(`Inserted ${insertedCount}/${shelters.length} shelters...`);
    }

    console.log(`✅ Finished! Seeded ${insertedCount} shelters into the database.`);
    process.exit(0);

  } catch (err) {
    console.error("Error seeding shelters:", err);
    process.exit(1);
  }
}

seedShelters();