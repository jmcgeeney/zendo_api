import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///zendo_dev.db")
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
    BACKFILL_START_DATE: str = os.getenv("BACKFILL_START_DATE", "2026-02-22")


settings = Settings()
