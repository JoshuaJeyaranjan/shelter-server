const axios = require('axios')
const packageId = '21c83b32-d5a8-4106-a54f-010dbe49f6f2'

async function fetchDatasetMetadata(packageId) {
  try {
    // 1️⃣ Fetch dataset info (this contains all resources)
    const { data: pkgData } = await axios.get(
      `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${packageId}`
    );

    if (!pkgData.success) {
      throw new Error("Failed to fetch package metadata from CKAN");
    }

    const dataset = pkgData.result;
    const resources = dataset.resources.filter(r => r.datastore_active);
    if (!resources.length) {
      throw new Error("No active datastore resources found in package");
    }

    // Take the first active datastore resource (the one you're already using)
    const resource = resources[0];

    // 2️⃣ Fetch record count + basic stats from datastore
    const { data: storeData } = await axios.get(
      "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search",
      { params: { id: resource.id, limit: 1 } }
    );

    const totalRecords = storeData.result.total;
    const lastModified = resource.last_modified || dataset.metadata_modified;
    const created = resource.created || dataset.metadata_created;

    console.log("\n📦 **Dataset Metadata**");
    console.log(`  📘 Title: ${dataset.title}`);
    console.log(`  🌐 Resource ID: ${resource.id}`);
    console.log(`  🕐 Created: ${created}`);
    console.log(`  🔁 Last Modified: ${lastModified}`);
    console.log(`  🧮 Total Records Available: ${totalRecords}`);
    console.log(`  🔗 Resource URL: ${resource.url}\n`);

    return {
      title: dataset.title,
      resourceId: resource.id,
      lastModified,
      totalRecords,
      created,
      resourceUrl: resource.url
    };
  } catch (err) {
    console.error("❌ Error fetching dataset metadata:", err.message);
    return null;
  }
}

fetchDatasetMetadata(packageId)