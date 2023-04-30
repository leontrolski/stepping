from dataclasses import dataclass
from functools import cache


@dataclass
class Config:
    DB_URL: str


@cache
def get_config() -> Config:
    return Config(DB_URL="not-a-postgres-url")
