require("dotenv").config();

const { seedLocations } = require("../utils/seedLocationsApi");
const { seedProgramsFromAPI } = require("../utils/seedProgramsApi");

async function runJob() {
  console.log("🌐 Starting CKAN shelter refresh job...");
  let failed = false;

  try {
    console.log("📍 Refreshing shelter locations from CKAN...");
    await seedLocations();
    console.log("✅ Locations refreshed successfully");
  } catch (err) {
    failed = true;
    console.error("❌ Location refresh failed:", err);
  }

  try {
    console.log("🛏️ Refreshing program occupancy from CKAN...");
    await seedProgramsFromAPI();
    console.log("✅ Programs refreshed successfully");
  } catch (err) {
    failed = true;
    console.error("❌ Program refresh failed:", err);
  }

  console.log("🌐 CKAN refresh job finished");

  process.exit(failed ? 1 : 0);
}

runJob();
