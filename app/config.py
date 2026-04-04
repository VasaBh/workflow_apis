from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Storage backend: "MONGO" or "FILES"
    DB: str = "MONGO"
    FILES_DATA_DIR: str = "./data"

    MONGODB_URL: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "workflowos"
    JWT_SECRET: str = "your-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30


settings = Settings()
