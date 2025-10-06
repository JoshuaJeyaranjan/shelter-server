require("dotenv").config();
const { seedLocationsFromAPI } = require("../seedLocationsFromApi");

async function runJob() {
  console.log("🌐 Starting shelter refresh job...");

  try {
    await seedLocationsFromAPI();
    console.log("✅ Shelter data refreshed successfully!");
  } catch (err) {
    console.error("❌ Error during shelter refresh:", err);
    process.exit(1); // indicate failure to Render
  } finally {
    console.log("🌐 Job finished.");
    process.exit(0); // exit cleanly
  }
}

// Run immediately
runJob();