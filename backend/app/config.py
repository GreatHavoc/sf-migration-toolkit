"""Backend configuration primitives."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    app_name: str = "Snowflake Migrator API"
    app_version: str = "0.1.0"
    api_prefix: str = "/api"
    default_mig_db: str = "MIGRATION_DB"
    default_mig_schema: str = "PUBLIC"
    default_stage: str = "MIGRATION_STAGE"
    default_integration: str = "AZURE_MIGRATION_INT"
    default_nb_int_stage: str = "NB_MIG_INT_STAGE"
    default_local_int_stage: str = "LOCAL_BACKUP_STAGE"
    data_dir: Path = Path("backend/data")
    database_file_name: str = "app.db"

    @property
    def database_path(self) -> Path:
        return self.data_dir / self.database_file_name


def load_settings() -> Settings:
    data_dir_env = os.getenv("SNOWFLAKE_MIGRATOR_DATA_DIR")
    data_dir = Path(data_dir_env) if data_dir_env else Path("backend/data")

    return Settings(
        app_name=os.getenv("SNOWFLAKE_MIGRATOR_APP_NAME", "Snowflake Migrator API"),
        app_version=os.getenv("SNOWFLAKE_MIGRATOR_APP_VERSION", "0.1.0"),
        api_prefix=os.getenv("SNOWFLAKE_MIGRATOR_API_PREFIX", "/api"),
        default_mig_db=os.getenv("SNOWFLAKE_MIGRATOR_DEFAULT_MIG_DB", "MIGRATION_DB"),
        default_mig_schema=os.getenv("SNOWFLAKE_MIGRATOR_DEFAULT_MIG_SCHEMA", "PUBLIC"),
        default_stage=os.getenv("SNOWFLAKE_MIGRATOR_DEFAULT_STAGE", "MIGRATION_STAGE"),
        default_integration=os.getenv(
            "SNOWFLAKE_MIGRATOR_DEFAULT_INTEGRATION", "AZURE_MIGRATION_INT"
        ),
        default_nb_int_stage=os.getenv(
            "SNOWFLAKE_MIGRATOR_DEFAULT_NB_INT_STAGE", "NB_MIG_INT_STAGE"
        ),
        default_local_int_stage=os.getenv(
            "SNOWFLAKE_MIGRATOR_DEFAULT_LOCAL_INT_STAGE", "LOCAL_BACKUP_STAGE"
        ),
        data_dir=data_dir,
        database_file_name=os.getenv("SNOWFLAKE_MIGRATOR_DB_FILE", "app.db"),
    )


settings = load_settings()
