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
      // Ensure numeric fields are numbers or null
      const serviceUserCount = s.service_user_count ? Number(s.service_user_count) : null;
      const capacityActualBed = s.capacity_actual_bed ? Number(s.capacity_actual_bed) : null;
      const capacityActualRoom = s.capacity_actual_room ? Number(s.capacity_actual_room) : null;
      const occupiedBeds = s.occupied_beds ? Number(s.occupied_beds) : null;
      const unoccupiedBeds = s.unoccupied_beds ? Number(s.unoccupied_beds) : null;
      const occupiedRooms = s.occupied_rooms ? Number(s.occupied_rooms) : null;
      const unoccupiedRooms = s.unoccupied_rooms ? Number(s.unoccupied_rooms) : null;

      await pool.query(insertQuery, [
        s.organization_name || null,
        s.shelter_group || null,
        s.location_name || null,
        s.address || null,
        s.postal_code || null,
        s.city || null,
        s.province || null,
        s.program_name || null,
        s.sector || null,
        s.overnight_service_type || null,
        serviceUserCount,
        capacityActualBed,
        capacityActualRoom,
        occupiedBeds,
        unoccupiedBeds,
        occupiedRooms,
        unoccupiedRooms,
        s.occupancy_date || null,
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