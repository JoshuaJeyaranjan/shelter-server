require("dotenv").config();
const cron = require("node-cron");
const { seedShelters } = require("../seedSheltersFromApi");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function runJob() {
  console.log("🌐 Connecting to database...");
  const client = await pool.connect();
  try {
    console.log("🔄 Running daily shelter refresh job...");
    await seedShelters(client);
    console.log("✅ Shelter data refreshed successfully");
  } catch (err) {
    console.error("❌ Error during refresh:", err);
  } finally {
    client.release();
  }
}

// Run once immediately (optional)
runJob();

// Schedule to run daily at 3 AM
cron.schedule("0 3 * * *", async () => {
  await runJob();
});