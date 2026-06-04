from pathlib import Path

import yaml

from app.schemas.settings_schema import Config

PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings:
    def __init__(
        self,
        config_path: str | Path = PROJECT_ROOT / "config.yaml",
    ):
        with open(config_path) as f:
            loaded_config = yaml.safe_load(f)
            self.__config = Config.model_validate(loaded_config)

    @property
    def project_root(self):
        return PROJECT_ROOT

    @property
    def config(self):
        return self.__config

    def __str__(self) -> str:
        return str(self.__config.model_dump())


settings = Settings()
