from asyncio import AbstractEventLoop

from sentry_sdk import init
from sentry_sdk.integrations.sanic import SanicIntegration
from yggdrasil.domain.exceptions import GalleryinfoNotFound, InfoNotFound
from yggdrasil.infrastructure.hitomila import HitomiLa
from yggdrasil.infrastructure.hitomila.repositories.galleryinfo import (
    HitomiLaGalleryinfoRepository,
)
from yggdrasil.infrastructure.mongodb import MongoDB
from yggdrasil.infrastructure.mongodb.repositories.info import MongoDBInfoRepository
from yggdrasil.infrastructure.sqlalchemy import SQLAlchemy
from yggdrasil.infrastructure.sqlalchemy.repositories.artist import SAArtistRepository
from yggdrasil.infrastructure.sqlalchemy.repositories.character import (
    SACharacterRepository,
)
from yggdrasil.infrastructure.sqlalchemy.repositories.galleryinfo import (
    SAGalleryinfoRepository,
)
from yggdrasil.infrastructure.sqlalchemy.repositories.group import SAGroupRepository
from yggdrasil.infrastructure.sqlalchemy.repositories.language_info import (
    SALanguageInfoRepository,
)
from yggdrasil.infrastructure.sqlalchemy.repositories.language_localname import (
    SALanguageLocalnameRepository,
)
from yggdrasil.infrastructure.sqlalchemy.repositories.parody import SAParodyRepository
from yggdrasil.infrastructure.sqlalchemy.repositories.tag import SATagRepository
from yggdrasil.infrastructure.sqlalchemy.repositories.type import SATypeRepository

from sunflower import __version__
from sunflower.adapters.endpoint import endpoint
from sunflower.application.tasks.manager import TaskManager
from sunflower.application.tasks.mirroring import MirroringTask
from sunflower.infrastructure.sanic.app import Sunflower
from sunflower.infrastructure.sanic.config import SunflowerConfig
from sunflower.infrastructure.sanic.error import not_found


async def startup(sunflower: Sunflower, loop: AbstractEventLoop) -> None:
    if sunflower.config.PRODUCTION:  # pragma: no cover
        init(
            sunflower.config.SENTRY_DSN,
            integrations=[SanicIntegration()],
            release=__version__,
            ignore_errors=[
                GalleryinfoNotFound,
                InfoNotFound,
            ],
            traces_sample_rate=1.0,
            profile_session_sample_rate=1.0,
            profile_lifecycle="trace",
        )

    sunflower.ctx.sa = SQLAlchemy.create(sunflower.config.GALLERYINFO_DB_URL)
    sunflower.ctx.hitomi_la = await HitomiLa.create(sunflower.config.INDEX_FILES)
    sunflower.ctx.mongodb = await MongoDB.create(sunflower.config.INFO_DB_URL)

    sunflower.ctx.hitomi_la_galleryinfo_repository = HitomiLaGalleryinfoRepository(
        sunflower.ctx.hitomi_la
    )
    sunflower.ctx.sa_galleryinfo_repository = SAGalleryinfoRepository(
        sunflower.ctx.sa,
        SATypeRepository(sunflower.ctx.sa),
        SAArtistRepository(sunflower.ctx.sa),
        SALanguageInfoRepository(sunflower.ctx.sa),
        SALanguageLocalnameRepository(sunflower.ctx.sa),
        SACharacterRepository(sunflower.ctx.sa),
        SAGroupRepository(sunflower.ctx.sa),
        SAParodyRepository(sunflower.ctx.sa),
        SATagRepository(sunflower.ctx.sa),
    )
    sunflower.ctx.mongodb_repository = MongoDBInfoRepository(
        sunflower.ctx.mongodb, sunflower.config.USE_ATLAS_SEARCH
    )

    task_manager = TaskManager(sunflower)

    await sunflower.ctx.sa.create_all_table()
    await sunflower.ctx.mongodb.collection.create_index([("id", -1)])
    if (
        sunflower.ctx.mongodb.is_atlas and sunflower.config.USE_ATLAS_SEARCH
    ):  # pragma: no cover
        await sunflower.ctx.mongodb.collection.create_search_index(
            {
                "name": "default",
                "definition": {
                    "mappings": {
                        "dynamic": True,
                        "fields": {
                            "title": {
                                "analyzer": "lucene.korean",
                                "searchAnalyzer": "lucene.korean",
                                "type": "string",
                            }
                        },
                    }
                },
            }
        )
    mirroring_task = MirroringTask(
        sunflower.ctx.hitomi_la_galleryinfo_repository,
        sunflower.ctx.sa_galleryinfo_repository,
        sunflower.ctx.mongodb_repository,
        sunflower.config.RUN_AS_ONCE,
    )
    mirroring_task.REMOTE_CONCURRENT_SIZE = (
        sunflower.config.MIRRORING_REMOTE_CONCURRENT_SIZE
    )
    mirroring_task.LOCAL_CONCURRENT_SIZE = (
        sunflower.config.MIRRORING_LOCAL_CONCURRENT_SIZE
    )
    mirroring_task.INTEGRITY_PARTIAL_CHECK_RANGE_SIZE = (
        sunflower.config.INTEGRITY_PARTIAL_CHECK_RANGE_SIZE
    )

    if not sunflower.test_mode:  # pragma: no cover
        if not sunflower.config.DISABLE_MIRRORING:
            task_manager.register_task(
                mirroring_task.start_mirroring,
                MirroringTask.__name__,
                sunflower.config.MIRRORING_DELAY,
            )
        if not sunflower.config.DISABLE_INTEGRITY_CHECK:
            if not sunflower.config.DISABLE_INTEGRITY_PARTIAL_CHECK:
                task_manager.register_task(
                    mirroring_task.start_partial_integrity_check,
                    f"{MirroringTask.__name__}_PartialIntegrityCheck",
                    sunflower.config.INTEGRITY_PARTIAL_CHECK_DELAY,
                )
            if not sunflower.config.DISABLE_INTEGRITY_FULL_CHECK:
                task_manager.register_task(
                    mirroring_task.start_full_integrity_check,
                    f"{MirroringTask.__name__}_FullIntegrityCheck",
                    sunflower.config.INTEGRITY_FULL_CHECK_DELAY,
                )


async def closeup(sunflower: Sunflower, loop: AbstractEventLoop) -> None:
    # Close session
    await sunflower.ctx.mongodb.client.close()
    await sunflower.ctx.sa.engine.dispose()
    await sunflower.ctx.hitomi_la.session.close()
    for task in sunflower.tasks:
        await sunflower.cancel_task(task.get_name())


def create_app(config: SunflowerConfig) -> Sunflower:
    sunflower = Sunflower("sunflower")
    sunflower.exception(  # pyright: ignore[reportUnknownMemberType]
        GalleryinfoNotFound, InfoNotFound
    )(not_found)
    sunflower.config.update(config)
    sunflower.blueprint(endpoint)
    sunflower.before_server_start(startup)
    sunflower.before_server_stop(closeup)

    return sunflower
