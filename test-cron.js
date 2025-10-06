require("dotenv").config();
const { seedShelters } = require("./seedSheltersFromApi");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function runJob() {
  console.log("🌐 Connecting to database...");
  const client = await pool.connect();
  try {
    console.log("🔄 Running daily shelter refresh job at", new Date().toLocaleString("en-CA", { timeZone: "America/Toronto" }));
    await seedShelters(client);
    console.log("✅ Shelter data refreshed successfully at", new Date().toLocaleString("en-CA", { timeZone: "America/Toronto" }));
  } catch (err) {
    console.error("❌ Error during refresh:", err);
  } finally {
    client.release();
    await pool.end();
  }
}

runJob();