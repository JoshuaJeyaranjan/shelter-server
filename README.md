# Toronto Shelters — API server

A small Express server that ingests Toronto open-data (CKAN) shelter datasets and exposes the latest occupancy and location information for shelters across Toronto.

Key ideas:
- Periodically fetches the CKAN datastore for shelter locations and program occupancy.
- Stores normalized rows in a Postgres database (`locations`, `programs`).
- Exposes a small REST API for listing locations, fetching a single location, occupancy details, and a mapping-friendly endpoint.

## Features

- Seed locations from the City of Toronto CKAN dataset (normalized and upserted).
- Seed latest program occupancy data from the CKAN datastore.
- Endpoints to query locations and occupancy, with simple filtering support.
- A CLI job to refresh both locations and programs (`jobs/refreshShelters.js`).

## Repository layout (important files)

- `index.js` — Express app entry.
- `config/db.js` — Postgres connection (uses `DATABASE_URL`).
- `routes/shelters.js` — API routes.
- `controllers/locationsController.js` — controller implementations for endpoints.
- `utils/seedLocationsApi.js` — fetch & upsert location rows from CKAN.
- `utils/seedProgramsApi.js` — fetch & upsert program occupancy rows from CKAN.
- `jobs/refreshShelters.js` — runs a full refresh (locations then programs).

## Requirements

- Node.js (18+ recommended)
- npm or yarn
- PostgreSQL database accessible via `DATABASE_URL`

The project expects a Postgres database. In production the `config/db.js` establishes the pool using the `DATABASE_URL` env var and enables SSL with `rejectUnauthorized: false`.

## Environment

Create a `.env` file at the project root with at least:

```
DATABASE_URL=postgres://user:pass@host:5432/dbname
PORT=3000  # optional
```

If you run a local Postgres for development you can use a connection string like `postgres://localhost:5432/shelters_dev`.

## Database schema (quick reference)

The code expects two primary tables: `locations` and `programs`. A minimal schema to get started:

```sql
-- locations
CREATE TABLE locations (
	id INTEGER PRIMARY KEY,
	organization_id INTEGER,
	organization_name TEXT,
	shelter_id INTEGER,
	shelter_group TEXT,
	location_name TEXT,
	address TEXT,
	postal_code TEXT,
	city TEXT,
	province TEXT,
	shelter_type TEXT,
	population_served TEXT,
	program_model TEXT,
	sector TEXT,
	created_at TIMESTAMP,
	last_refreshed TIMESTAMP
);

-- programs
CREATE TABLE programs (
	id SERIAL PRIMARY KEY,
	location_id INTEGER REFERENCES locations(id),
	program_name TEXT,
	sector TEXT,
	overnight_service_type TEXT,
	service_user_count INTEGER,
	capacity_actual_bed INTEGER,
	occupied_beds INTEGER,
	unoccupied_beds INTEGER,
	capacity_actual_room INTEGER,
	occupied_rooms INTEGER,
	unoccupied_rooms INTEGER,
	occupancy_date DATE,
	last_refreshed TIMESTAMP,
	UNIQUE(location_id, program_name)
);


Adjust types/constraints as needed for your environment.

## Install

```bash
# install deps
npm install

# start server (development)
npm run dev # if you have a script set up; otherwise `node index.js`
```

If your `package.json` doesn't have a `dev` script, start with:

```bash
node index.js
```

## Seeding / Refreshing data

There are two utility scripts that fetch data from the City of Toronto CKAN API and upsert into Postgres.

- Seed locations (from CKAN): `utils/seedLocationsApi.js`
- Seed programs + occupancy (from CKAN): `utils/seedProgramsApi.js`

Run the combined refresh job (recommended) which runs both in sequence:

```bash
# from project root
node jobs/refreshShelters.js
```

You can run each seed script directly:

```bash
node utils/seedLocationsApi.js
node utils/seedProgramsApi.js
```

Notes:
- The CKAN resource id used by the scripts is defined within each `utils/*` file. The code paginates the datastore and attempts to upsert all records.
- `seedProgramsApi.js` takes the latest occupancy date from the datastore and inserts programs for that date.

## API Endpoints

Base path: `/locations`

- GET /locations
	- Returns all locations and their programs (if any). Supports optional query parameters:
		- `city` — exact match filter on the `city` column
		- `sector` — filters program sector
		- `minVacancyBeds` — numeric filter for minimum available beds
		- `minVacancyRooms` — numeric filter for minimum available rooms

- GET /locations/map
	- Returns an array of locations that include `latitude` and `longitude` for mapping. Programs for each location are also included.

- GET /locations/:id
	- Returns full location details and all programs for that location.

- GET /locations/:id/occupancy
	- Returns the location with programs, each program including occupancy fields and computed unoccupied counts and occupancy rates.

Example using curl:

```bash
# list locations
curl "http://localhost:3000/locations"

# list locations in Toronto
curl "http://localhost:3000/locations?city=Toronto"

# get map-ready locations
curl "http://localhost:3000/locations/map"

# get a specific location
curl "http://localhost:3000/locations/123"

# get occupancy for a location
curl "http://localhost:3000/locations/123/occupancy"
```

## Development notes & assumptions

- The CKAN datastore resource id is currently hard-coded in the seed scripts. Change the constant if the dataset moves.
- The seed scripts perform upserts and set `last_refreshed` timestamps in `America/Toronto` timezone.
- `config/db.js` configures the pg Pool with SSL enabled (rejectUnauthorized: false) — change this for local development if needed.
- No migrations are included — you can create the tables manually or use your preferred migration tool.

## Troubleshooting

- If seed scripts hang or fail, check connectivity to the CKAN URL and your `DATABASE_URL`.
- For SSL errors against Postgres hosted services, double-check the `ssl` settings in `config/db.js` or provide the required certificates.

## Next steps / Improvements

- Add proper DB migrations (eg. using `node-pg-migrate` or `knex`) and tests.
- Add caching and pagination to the API endpoints for large result sets.
- Add docker-compose for a local Postgres for easier developer onboarding.

## License

MIT — feel free to use and modify.

