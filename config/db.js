require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // use the Render external URL
  ssl: { rejectUnauthorized: false },
});

module.exports = pool;