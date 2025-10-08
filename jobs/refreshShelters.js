require("dotenv").config();
const { seedLocationsApi } = require("../seedLocationsFromApi");
const { seedProgramsApi } = require("../seedProgramsApi");

async function runJob() {
  console.log("🌐 Starting shelter & program refresh job...");

  let failed = false;

  // Step 1: Refresh shelter locations
  try {
    console.log("🚀 Refreshing shelter locations...");
    await seedLocationsApi();
    console.log("✅ Shelter locations refreshed successfully!");
  } catch (err) {
    failed = true;
    console.error("❌ Error refreshing shelter locations:", err);
  }

  // Step 2: Seed programs
  try {
    console.log("🚀 Seeding program data...");
    await seedProgramsApi();
    console.log("✅ Program data seeded successfully!");
  } catch (err) {
    failed = true;
    console.error("❌ Error seeding program data:", err);
  }

  console.log("🌐 Job finished.");

  if (failed) {
    process.exit(1); // indicate failure to Render
  } else {
    process.exit(0); // exit cleanly
  }
}

// Run immediately
runJob();