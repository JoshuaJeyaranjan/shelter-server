require("dotenv").config();

const { seedLocations } = require("../seedLocationsApi");
const { seedProgramsFromAPI } = require("../seedProgramsApi");

async function runJob() {
  console.log("🌐 Starting CKAN shelter refresh job...");
  let failed = false;

  // Step 1: Refresh locations
  try {
    console.log("📍 Refreshing shelter locations from CKAN...");
    await seedLocations();
    console.log("✅ Locations refreshed successfully");
  } catch (err) {
    failed = true;
    console.error("❌ Location refresh failed:", err);
  }

  // Step 2: Refresh programs (latest occupancy snapshot)
  try {
    console.log("🛏️ Refreshing program occupancy from CKAN...");
    await seedProgramsFromAPI();
    console.log("✅ Programs refreshed successfully");
  } catch (err) {
    failed = true;
    console.error("❌ Program refresh failed:", err);
  }

  console.log("🌐 CKAN refresh job finished");

  // Signal success/failure to Render / cron supervisor
  process.exit(failed ? 1 : 0);
}

// Run immediately when invoked
runJob();