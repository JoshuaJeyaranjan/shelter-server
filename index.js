require("dotenv").config();
const express = require("express");
const cors = require("cors");
const morgan = require("morgan");

const shelterRoutes = require("./routes/shelters");

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

// Routes
app.use("/locations", shelterRoutes);

app.get("/", (req, res) => {
  res.send("Toronto Shelters API is running!");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});