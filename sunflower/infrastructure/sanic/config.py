from argparse import Namespace
from json import loads
from typing import Any, Callable, Optional, Sequence, Union, cast

from sanic.config import Config

from sunflower import __version__

SUNFLOWER_PREFIX = "SUNFLOWER_"


def list_converter(value: str) -> list[Any]:
    if value.startswith("["):
        return cast(list[Any], loads(value))
    raise ValueError


class SunflowerConfig(Config):
    def __init__(
        self,
        defaults: dict[str, Union[str, bool, int, float, None]] = {},
        env_prefix: Optional[str] = SUNFLOWER_PREFIX,
        keep_alive: Optional[bool] = None,
        *,
        converters: Optional[Sequence[Callable[[str], Any]]] = [list_converter],
    ):
        # Default
        self.update(
            {
                # sunflower
                "CONFIG": "",
                "PRODUCTION": False,
                "USE_ENV": False,
                "SENTRY_DSN": "",
                "GALLERYINFO_DB_URL": "",
                "INFO_DB_URL": "",
                "INDEX_FILES": ["index-english.nozomi"],
                "MIRRORING_DELAY": 3600,
                "INTEGRITY_PARTIAL_CHECK_DELAY": 21600,
                "INTEGRITY_FULL_CHECK_DELAY": 432000,
                "USE_ATLAS_SEARCH": False,
                "MIRRORING_REMOTE_CONCURRENT_SIZE": 50,
                "MIRRORING_LOCAL_CONCURRENT_SIZE": 25,
                "INTEGRITY_PARTIAL_CHECK_RANGE_SIZE": 100,
                "RUN_AS_ONCE": False,
                "DISABLE_MIRRORING": False,
                "DISABLE_INTEGRITY_CHECK": False,
                "DISABLE_INTEGRITY_PARTIAL_CHECK": False,
                "DISABLE_INTEGRITY_FULL_CHECK": False,
                # Sanic config
                "HOST": "127.0.0.1",
                "PORT": 8000,
                "DEBUG": False,
                "ACCESS_LOG": False,
                "FORWARDED_SECRET": "",
                # Sanic ext config
                "OAS_UI_DEFAULT": "swagger",
                "OAS_URI_REDOC": False,
                # Open API config
                "SWAGGER_UI_CONFIGURATION": {
                    "apisSorter": "alpha",
                    "operationsSorter": "alpha",
                },
                "API_TITLE": "Sunflower",
                "API_VERSION": __version__,
                "API_DESCRIPTION": "Hitomi.la mirror api",
                "API_LICENSE_NAME": "MIT",
                "API_LICENSE_URL": "https://github.com/Saebasol/Sunflower/blob/main/LICENSE",
            }
        )
        super().__init__(
            defaults={**{"_FALLBACK_ERROR_FORMAT": "json"}, **defaults},
            env_prefix=env_prefix,
            keep_alive=keep_alive,
            converters=converters,
        )

    # Sunflower
    USE_ENV: bool
    CONFIG: str
    PRODUCTION: bool
    SENTRY_DSN: str
    GALLERYINFO_DB_URL: str
    INFO_DB_URL: str
    MIRRORING_DELAY: float
    INTEGRITY_PARTIAL_CHECK_DELAY: float
    INTEGRITY_FULL_CHECK_DELAY: float
    INDEX_FILES: list[str]
    USE_ATLAS_SEARCH: bool
    MIRRORING_REMOTE_CONCURRENT_SIZE: int
    MIRRORING_LOCAL_CONCURRENT_SIZE: int
    INTEGRITY_PARTIAL_CHECK_RANGE_SIZE: int
    RUN_AS_ONCE: bool
    DISABLE_MIRRORING: bool
    DISABLE_INTEGRITY_CHECK: bool
    DISABLE_INTEGRITY_PARTIAL_CHECK: bool
    DISABLE_INTEGRITY_FULL_CHECK: bool
    # Sanic config
    DEBUG: bool
    HOST: str
    PORT: int

    def load_config_with_config_json(self, path: str) -> None:
        with open(path, "r") as f:
            config = loads(f.read())
            self.update_config(config)  # pyright: ignore[reportUnknownMemberType]
        return None

    def update_with_args(self, args: Namespace) -> None:
        if not self.USE_ENV:
            self.update_config(  # pyright: ignore[reportUnknownMemberType]
                {k.upper(): v for k, v in vars(args).items()}
            )
        if self.CONFIG:
            self.load_config_with_config_json(self.CONFIG)
        return None
