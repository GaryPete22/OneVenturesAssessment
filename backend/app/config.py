from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RECON_")

    data_dir: Path = Path(__file__).resolve().parent.parent.parent
    amount_tolerance: float = 0.01
    settlement_window_min_days: int = 1
    settlement_window_max_days: int = 3
    rounding_aggregate_threshold: float = 1.0


settings = Settings()
