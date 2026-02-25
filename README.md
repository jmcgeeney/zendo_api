# Zendemo

Datacenter-monitoring API platform, complete with ETL orchestration, a robust API exposing production, consumption, and weather data, and an expertly vibe-coded frontend to provide clients with insights into their daily energy profile.

---

## Project Structure

```
api/          FastAPI app — routes, services, simulators, DB client
etl/          ETL scripts for weather, irradiance, production, consumption, correlations
lib/          Shared utils
web/          React + TypeScript frontend
tests/        I'll give you one guess what this is
```

---

## Setup

### Prerequisites

- Python 3.11+
- Node.js 20+
- An [OpenWeather API key](https://openweathermap.org/api)

### Environment
Once you have an OpenWeather API key, set your environment variables like so: (backfill date and database url are up to your discretion)

```bash
pip install -r api/requirements.txt

export PYTHONPATH=$PYTHONPATH:<path/to/zendo_api>
export OPENWEATHER_API_KEY=<your_api_key>
export BACKFILL_START_DATE=<lookback_date_for_data>
export DATABASE_URL=sqlite:///<sqlite_db_path>
```

### Data
Before you can start, initialize your DB (and create your first customer) with...

```bash
python api/db/init_db.py
```

...then fetch some data:
```bash
python etl/orchestration.py --date <any_date>
```

## Testing & Development

### Backend
Start your backend server:

```bash
uvicorn api.main:app --reload
```

### Frontend
Then start the frontend:

```bash
cd web
npm install
npm run dev
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/customers` | List all customers |
| `GET` | `/api/customers/{id}/weather` | Current weather at customer location |
| `GET` | `/api/customer/{id}/energy-summary/{date}` | Daily production, consumption, net kWh, and correlations |
| `GET` | `/api/customer/{id}/historical-data/{date}` | Full 15-minute time series for a given date. Includes solar irradiance, temperature, consumption, and production values |

---

## Running Tests

To test, simply run this from your top-level directory:
```bash
pytest
```
