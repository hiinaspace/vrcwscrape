"""Configuration management for VRChat scraper."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Application configuration from environment variables."""

    database_url: str = Field(..., description="MySQL connection string")
    vrchat_auth_cookie: str = Field(..., description="VRChat auth cookie")
    image_storage_path: str = Field("./images", description="Path to store images")
    log_level: str = Field("INFO", description="Logging level")
    oneshot_max_worlds: int = Field(
        2500, description="Maximum number of worlds to scrape in oneshot mode"
    )
    oneshot_manage_dolt: bool = Field(
        False,
        description="Whether to start/stop Dolt SQL server in oneshot mode (useful for working around memory leaks during large backlogs)",
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }
