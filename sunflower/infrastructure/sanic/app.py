from types import SimpleNamespace
from typing import Any

from sanic.app import Sanic
from sanic.request import Request
from yggdrasil.infrastructure.hitomila import HitomiLa
from yggdrasil.infrastructure.hitomila.repositories.galleryinfo import (
    HitomiLaGalleryinfoRepository,
)
from yggdrasil.infrastructure.mongodb import MongoDB
from yggdrasil.infrastructure.mongodb.repositories.info import MongoDBInfoRepository
from yggdrasil.infrastructure.sqlalchemy import SQLAlchemy
from yggdrasil.infrastructure.sqlalchemy.repositories.galleryinfo import (
    SAGalleryinfoRepository,
)

from sunflower.application.tasks.mirroring import MirroringTask
from sunflower.infrastructure.sanic.config import SunflowerConfig


class SunflowerContext(SimpleNamespace):
    sa: SQLAlchemy
    hitomi_la: HitomiLa
    mongodb: MongoDB
    sa_galleryinfo_repository: SAGalleryinfoRepository
    hitomi_la_galleryinfo_repository: HitomiLaGalleryinfoRepository
    mongodb_repository: MongoDBInfoRepository
    mirroring_task: MirroringTask


class Sunflower(Sanic[SunflowerConfig, SunflowerContext]): ...


class SunflowerRequest(Request):
    app: Sunflower
    args: property
    json: Any
