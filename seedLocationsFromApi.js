require('dotenv').config()
const axios = require('axios')
const pool = require('./config/db')

const PACKAGE_ID = '21c83b32-d5a8-4106-a54f-010dbe49f6f2'
const BATCH_SIZE = 500

// Normalize API record
function normalizeRecord(r) {
  if (!r._id) return null
  return {
    id: r._id,
    shelter_id: r.SHELTER_ID || null,
    organization_name: r.ORGANIZATION_NAME || null,
    shelter_group: r.SHELTER_GROUP || null,
    location_name: r.LOCATION_NAME || null,
    address: r.LOCATION_ADDRESS || null,
    postal_code: r.LOCATION_POSTAL_CODE || null,
    city: r.LOCATION_CITY || null,
    province: r.LOCATION_PROVINCE || null,
    latitude: r.LATITUDE ? parseFloat(r.LATITUDE) : null,
    longitude: r.LONGITUDE ? parseFloat(r.LONGITUDE) : null,
    programs: [{
      id: r._id,
      program_name: r.PROGRAM_NAME || null,
      sector: r.SECTOR || null,
      overnight_service_type: r.OVERNIGHT_SERVICE_TYPE || null,
      service_user_count: r.SERVICE_USER_COUNT ? parseInt(r.SERVICE_USER_COUNT) : null,
      capacity_actual_bed: r.CAPACITY_ACTUAL_BED ? parseInt(r.CAPACITY_ACTUAL_BED) : null,
      capacity_actual_room: r.CAPACITY_ACTUAL_ROOM ? parseInt(r.CAPACITY_ACTUAL_ROOM) : null,
      occupied_beds: r.OCCUPIED_BEDS ? parseInt(r.OCCUPIED_BEDS) : null,
      unoccupied_beds: r.UNOCCUPIED_BEDS ? parseInt(r.UNOCCUPIED_BEDS) : null,
      occupied_rooms: r.OCCUPIED_ROOMS ? parseInt(r.OCCUPIED_ROOMS) : null,
      unoccupied_rooms: r.UNOCCUPIED_ROOMS ? parseInt(r.UNOCCUPIED_ROOMS) : null,
      occupancy_date: r.OCCUPANCY_DATE || null
    }]
  }
}

// Deduplicate by location
function deduplicateLocations(records) {
  const locationMap = new Map()
  for (const r of records) {
    if (!r.location_name || !r.address) continue
    const key = `${r.location_name}||${r.address}||${r.city}||${r.province}`.toLowerCase()
    if (!locationMap.has(key)) {
      locationMap.set(key, { ...r, programs: [...r.programs] })
    } else {
      // Merge programs
      locationMap.get(key).programs.push(...r.programs)
    }
  }
  return Array.from(locationMap.values())
}

// Fetch all records from CKAN API
async function fetchAllRecords(resourceId) {
  const limit = 5000
  let offset = 0
  let allRecords = []
  let totalCount = 0

  do {
    const { data } = await axios.get(
      'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search',
      { params: { id: resourceId, limit, offset } }
    )
    const records = data.result.records.map(normalizeRecord).filter(Boolean)
    allRecords = allRecords.concat(records)
    totalCount = data.result.total
    offset += limit
    console.log(`📦 Fetched ${allRecords.length} / ${totalCount} records...`)
  } while (offset < totalCount)

  return allRecords
}

// Insert locations
async function insertLocations(client, locations) {
  if (!locations.length) return
  const queryText = `
    INSERT INTO locations (
      location_name, address, postal_code, city, province, latitude, longitude
    ) VALUES ${locations.map(
      (_, i) => `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, $${i * 7 + 7})`
    ).join(',')}
    ON CONFLICT (location_name, address, city, province) DO NOTHING
    RETURNING id, location_name, address, city, province
  `
  const values = locations.flatMap(l => [
    l.location_name,
    l.address,
    l.postal_code,
    l.city,
    l.province,
    l.latitude,
    l.longitude
  ])
  const res = await client.query(queryText, values)
  // Map returned IDs to locations for program insert
  const idMap = new Map()
  res.rows.forEach(r => {
    const key = `${r.location_name}||${r.address}||${r.city}||${r.province}`.toLowerCase()
    idMap.set(key, r.id)
  })
  return idMap
}

// Insert programs
async function insertPrograms(client, locations, idMap) {
  const programs = []
  locations.forEach(loc => {
    const locKey = `${loc.location_name}||${loc.address}||${loc.city}||${loc.province}`.toLowerCase()
    const locId = idMap.get(locKey)
    if (!locId) return
    loc.programs.forEach(p => {
      programs.push([
        p.program_name,
        p.sector,
        p.overnight_service_type,
        p.service_user_count,
        p.capacity_actual_bed,
        p.occupied_beds,
        p.unoccupied_beds,
        p.capacity_actual_room,
        p.occupied_rooms,
        p.unoccupied_rooms,
        p.occupancy_date,
        locId
      ])
    })
  })

  for (let i = 0; i < programs.length; i += BATCH_SIZE) {
    const batch = programs.slice(i, i + BATCH_SIZE)
    const queryText = `
      INSERT INTO programs (
        program_name, sector, overnight_service_type, service_user_count,
        capacity_actual_bed, occupied_beds, unoccupied_beds,
        capacity_actual_room, occupied_rooms, unoccupied_rooms,
        occupancy_date, location_id
      ) VALUES ${batch.map(
        (_, j) => `(${Array.from({ length: 12 }, (_, k) => `$${j * 12 + k + 1}`).join(',')})`
      ).join(',')}
      ON CONFLICT (location_id, program_name) DO UPDATE SET
        sector = EXCLUDED.sector,
        overnight_service_type = EXCLUDED.overnight_service_type,
        service_user_count = EXCLUDED.service_user_count,
        capacity_actual_bed = EXCLUDED.capacity_actual_bed,
        occupied_beds = EXCLUDED.occupied_beds,
        unoccupied_beds = EXCLUDED.unoccupied_beds,
        capacity_actual_room = EXCLUDED.capacity_actual_room,
        occupied_rooms = EXCLUDED.occupied_rooms,
        unoccupied_rooms = EXCLUDED.unoccupied_rooms,
        occupancy_date = EXCLUDED.occupancy_date
    `
    const values = batch.flat()
    await client.query(queryText, values)
    console.log(`📝 Inserted/Updated ${i + batch.length} / ${programs.length} programs...`)
  }
}

// Main seed function
async function seedLocationsFromAPI() {
  const client = await pool.connect()
  try {
    console.log('🌐 Connecting to database...')
    const { data: pkgData } = await axios.get(
      `https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=${PACKAGE_ID}`
    )
    const resources = pkgData.result.resources.filter(r => r.datastore_active)
    if (!resources.length) throw new Error('No active datastore resources found')
    const resourceId = resources[0].id

    const allRecords = await fetchAllRecords(resourceId)
    console.log(`📦 Total fetched: ${allRecords.length}`)

    const locations = deduplicateLocations(allRecords)
    console.log(`✅ Total locations after deduplication: ${locations.length}`)

    const idMap = await insertLocations(client, locations)
    await insertPrograms(client, locations, idMap)

    await client.query(`
      INSERT INTO shelter_metadata (id, last_refreshed)
      VALUES (1, NOW())
      ON CONFLICT (id) DO UPDATE SET last_refreshed = EXCLUDED.last_refreshed
    `)

    console.log('🎉 Seeding complete!')
  } catch (err) {
    console.error('❌ Error seeding locations:', err)
  } finally {
    client.release()
    await pool.end()
  }
}

module.exports = { seedLocationsFromAPI }

if (require.main === module) {
  seedLocationsFromAPI()
}