// // seedSheltersFromApi.js
// require("dotenv").config();
// const axios = require("axios");
// const pool = require("./config/db");

// const PACKAGE_ID = "21c83b32-d5a8-4106-a54f-010dbe49f6f2";
// const BATCH_SIZE = 500;

// // Normalize each record
// function normalizeRecord(r) {
//   return {
//     shelter_id: r.SHELTER_ID || null,
//     organization_name: r.ORGANIZATION_NAME || null,
//     shelter_group: r.SHELTER_GROUP || null,
//     location_name: r.LOCATION_NAME || null,
//     address: r.LOCATION_ADDRESS || null,
//     postal_code: r.LOCATION_POSTAL_CODE || null,
//     city: r.LOCATION_CITY || null,
//     province: r.LOCATION_PROVINCE || null,
//     program_name: r.PROGRAM_NAME || null,
//     sector: r.SECTOR || null,
//     overnight_service_type: r.OVERNIGHT_SERVICE_TYPE || null,
//     service_user_count: r.SERVICE_USER_COUNT
//       ? parseInt(r.SERVICE_USER_COUNT)
//       : null,
//     capacity_actual_bed: r.CAPACITY_ACTUAL_BED
//       ? parseInt(r.CAPACITY_ACTUAL_BED)
//       : null,
//     capacity_actual_room: r.CAPACITY_ACTUAL_ROOM
//       ? parseInt(r.CAPACITY_ACTUAL_ROOM)
//       : null,
//     occupied_beds: r.OCCUPIED_BEDS ? parseInt(r.OCCUPIED_BEDS) : null,
//     unoccupied_beds: r.UNOCCUPIED_BEDS ? parseInt(r.UNOCCUPIED_BEDS) : null,
//     occupied_rooms: r.OCCUPIED_ROOMS ? parseInt(r.OCCUPIED_ROOMS) : null,
//     unoccupied_rooms: r.UNOCCUPIED_ROOMS ? parseInt(r.UNOCCUPIED_ROOMS) : null,
//     occupancy_date: r.OCCUPANCY_DATE || null,
//     latitude: r.LATITUDE || null,
//     longitude: r.LONGITUDE || null,
//   };
// }

// // Fetch all records via pagination
// async function fetchAllRecords(resourceId) {
//   const limit = 5000;
//   let offset = 0;
//   let allRecords = [];
//   let totalCount = 0;

//   do {
//     const { data } = await axios.get(
//       "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search",
//       {
//         params: { id: resourceId, limit, offset },
//       }
//     );

//     const records = data.result.records.map(normalizeRecord);
//     allRecords = allRecords.concat(records);
//     totalCount = data.result.total;
//     offset += limit;

//     console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`);
//   } while (offset < totalCount);

//   return allRecords;
// }

// // Batch insert into Postgres
// async function insertBatch(client, batch) {
//   if (!batch.length) return;

//   const uniqueBatch = Object.values(
//     batch.reduce((acc, r) => {
//       acc[r.shelter_id] = r;
//       return acc;
//     }, {})
//   );

//   const queryText = `
//     INSERT INTO shelters (
//       shelter_id, organization_name, shelter_group, location_name, address, postal_code, city, province,
//       program_name, sector, overnight_service_type, service_user_count,
//       capacity_actual_bed, capacity_actual_room, occupied_beds, unoccupied_beds,
//       occupied_rooms, unoccupied_rooms, occupancy_date, latitude, longitude
//     ) VALUES ${uniqueBatch
//       .map(
//         (_, i) =>
//           `(${Array.from({ length: 21 }, (_, j) => `$${i * 21 + j + 1}`).join(",")})`
//       )
//       .join(",")}
//     ON CONFLICT (shelter_id) DO UPDATE SET
//       organization_name = EXCLUDED.organization_name,
//       shelter_group = EXCLUDED.shelter_group,
//       location_name = EXCLUDED.location_name,
//       address = EXCLUDED.address,
//       postal_code = EXCLUDED.postal_code,
//       city = EXCLUDED.city,
//       province = EXCLUDED.province,
//       program_name = EXCLUDED.program_name,
//       sector = EXCLUDED.sector,
//       overnight_service_type = EXCLUDED.overnight_service_type,
//       service_user_count = EXCLUDED.service_user_count,
//       capacity_actual_bed = EXCLUDED.capacity_actual_bed,
//       capacity_actual_room = EXCLUDED.capacity_actual_room,
//       occupied_beds = EXCLUDED.occupied_beds,
//       unoccupied_beds = EXCLUDED.unoccupied_beds,
//       occupied_rooms = EXCLUDED.occupied_rooms,
//       unoccupied_rooms = EXCLUDED.unoccupied_rooms,
//       occupancy_date = EXCLUDED.occupancy_date,
//       latitude = EXCLUDED.latitude,
//       longitude = EXCLUDED.longitude;
//   `;

//   const values = uniqueBatch.flatMap(r => [
//     r.shelter_id,
//     r.organization_name,
//     r.shelter_group,
//     r.location_name,
//     r.address,
//     r.postal_code,
//     r.city,
//     r.province,
//     r.program_name,
//     r.sector,
//     r.overnight_service_type,
//     r.service_user_count,
//     r.capacity_actual_bed,
//     r.capacity_actual_room,
//     r.occupied_beds,
//     r.unoccupied_beds,
//     r.occupied_rooms,
//     r.unoccupied_rooms,
//     r.occupancy_date,
//     r.latitude,
//     r.longitude,
//   ]);

//   await client.query(queryText, values);
// }

// // ✅ Main seeding function
// async function seedShelters(clientFromCron = null) {
//   const client = clientFromCron || (await pool.connect());
//   try {
//     console.log("🌐 Connecting to database...");

//     const { data: pkgData } = await axios.get(
//       `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${PACKAGE_ID}`
//     );

//     const resources = pkgData.result.resources.filter(r => r.datastore_active);
//     if (!resources.length) throw new Error("No active datastore resources found");

//     const resourceId = resources[0].id;
//     const records = await fetchAllRecords(resourceId);
//     console.log(`✅ Total records fetched: ${records.length}`);

//     for (let i = 0; i < records.length; i += BATCH_SIZE) {
//       const batch = records.slice(i, i + BATCH_SIZE);
//       await insertBatch(client, batch);
//       console.log(`📝 Inserted/Updated ${i + batch.length} / ${records.length} shelters...`);
//     }

//     console.log("🎉 Seeding complete!");
//   } catch (err) {
//     console.error("❌ Error seeding shelters:", err);
//   } finally {
//     if (!clientFromCron) {
//       client.release();
//       await pool.end();
//     }
//   }
// }

// module.exports = { seedShelters };

// if (require.main === module) {
//   seedShelters();
// }