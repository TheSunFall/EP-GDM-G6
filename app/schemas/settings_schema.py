from typing import Literal, Optional

from pydantic import BaseModel


class DatasetDictionary(BaseModel):
    filename: str
    path: Optional[str] = "dicts"
    id: str


class DatasetModule(BaseModel):
    name: str
    type: Literal["full", "monthly", "daily"] = "full"
    id: Optional[str] = None
    dictionary: Optional[list[DatasetDictionary]] = None


class Dataset(BaseModel):
    name: str
    full_name: str
    type: Literal["api", "fs"]
    description: str
    format: str
    save_format: str
    url: str
    dictionary: Optional[list[DatasetDictionary]] = None
    modules: list[DatasetModule]


class ApiConfig(BaseModel):
    path: str
    timeout: int


class LogsConfig(BaseModel):
    path: str


class Config(BaseModel):
    logs: LogsConfig
    api: ApiConfig
    datasets: list[Dataset]
