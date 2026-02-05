require("dotenv").config();
const express = require("express");
const cors = require("cors");
const morgan = require("morgan");

const shelterRoutes = require("./routes/shelters");

const app = express();
const PORT = process.env.PORT || 3000;
const pool = require("./config/db");

// Middleware
app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

// Routes
app.use("/locations", shelterRoutes);

app.get('/debug/db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    console.log('DATABASE_URL:', process.env.DATABASE_URL);
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    console.log('DATABASE_URL:', process.env.DATABASE_URL);
    res.status(500).json({ error: 'DB connection failed' });
  }
});


app.get("/", (req, res) => {
  res.send("Toronto Shelters API is running!");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});