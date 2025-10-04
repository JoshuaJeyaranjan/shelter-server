const https = require("https");

const packageId = "21c83b32-d5a8-4106-a54f-010dbe49f6f2";

// Fetch package metadata
https.get(
  `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${packageId}`,
  (response) => {
    let dataChunks = [];

    response.on("data", (chunk) => {
      dataChunks.push(chunk);
    });

    response.on("end", () => {
      const data = Buffer.concat(dataChunks).toString();
      const pkg = JSON.parse(data)["result"];
      console.log("Package name:", pkg.name);
      console.log("Number of resources:", pkg.resources.length);

      // Fetch the first datastore resource
      const datastoreResource = pkg.resources.find((r) => r.datastore_active);
      if (!datastoreResource) {
        console.log("No active datastore resource found.");
        return;
      }

      https.get(
        `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search?id=${datastoreResource.id}&limit=5`,
        (res) => {
          let chunks = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => {
            const records = JSON.parse(Buffer.concat(chunks).toString())["result"]["records"];
            console.log("Sample records:", records);
          });
        }
      ).on("error", (err) => console.error("Error fetching resource data:", err));
    });
  }
).on("error", (err) => console.error("Error fetching package metadata:", err));