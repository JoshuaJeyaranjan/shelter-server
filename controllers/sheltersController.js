const pool = require("../config/db");

// GET /api/shelters
exports.getAllShelters = async (req, res, next) => {
  try {
    const { sector, city } = req.query;
    let query = "SELECT * FROM shelters";
    const params = [];

    if (sector && city) {
      query += " WHERE sector = $1 AND city = $2";
      params.push(sector, city);
    } else if (sector) {
      query += " WHERE sector = $1";
      params.push(sector);
    } else if (city) {
      query += " WHERE city = $1";
      params.push(city);
    }

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/:id
exports.getShelterById = async (req, res, next) => {
  try {
    const { id } = req.params;
    const result = await pool.query("SELECT * FROM shelters WHERE id = $1", [id]);
    if (result.rows.length === 0) return res.status(404).json({ message: "Shelter not found" });
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
};

// GET /api/shelters/map
exports.getSheltersForMap = async (req, res, next) => {
  try {
    const result = await pool.query(
      "SELECT id, location_name, address, city, province FROM shelters"
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
};