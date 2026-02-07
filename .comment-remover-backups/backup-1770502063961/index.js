import "dotenv/config";
import express from "express";
import cors from "cors";
import morgan from "morgan";

import pool from "./config/db.js";
import shelterRoutes from "./routes/locations.js";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

app.use("/locations", shelterRoutes);

app.get("/", (req, res) => {
  res.send("Toronto Shelters API is running!");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
