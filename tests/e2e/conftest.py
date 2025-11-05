import httpx
import pytest_asyncio
from sanic import Sanic
from yggdrasil.domain.entities.galleryinfo import Galleryinfo
from yggdrasil.domain.entities.info import Info

from sunflower.infrastructure.sanic.app import Sunflower
from sunflower.infrastructure.sanic.bootstrap import create_app
from sunflower.infrastructure.sanic.config import SunflowerConfig
from tests.conftest import *

ASGI_HOST = "mockserver"
ASGI_PORT = 1234
ASGI_BASE_URL = f"http://{ASGI_HOST}:{ASGI_PORT}"
HOST = "127.0.0.1"
PORT = None


@pytest_asyncio.fixture()
async def sunflower():
    Sanic.test_mode = True
    config = SunflowerConfig()
    config.load_config_with_config_json("tests/config.json")
    sunflower = create_app(config)
    sunflower.asgi = True
    yield sunflower
    Sanic.test_mode = False


@pytest_asyncio.fixture()
async def asgi_client(
    sunflower: Sunflower, sample_galleryinfo: Galleryinfo, sample_info: Info
):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=sunflower, client=(ASGI_HOST, ASGI_PORT)),
        base_url=ASGI_BASE_URL,
        headers={"Host": f"{ASGI_HOST}:{ASGI_PORT}"},
    ) as client:
        sunflower.router.reset()
        sunflower.signal_router.reset()
        await sunflower._startup()
        await sunflower._server_event("init", "before")
        await sunflower._server_event("init", "after")
        # Add sample data to the repositories
        await sunflower.ctx.sa_galleryinfo_repository.add_galleryinfo(
            sample_galleryinfo
        )
        await sunflower.ctx.mongodb_repository.add_info(sample_info)
        yield client
        # Clean up the sample data from the repositories
        await sunflower.ctx.sa_galleryinfo_repository.delete_galleryinfo(
            sample_galleryinfo.id
        )
        await sunflower.ctx.mongodb_repository.delete_info(sample_info.id)
        await sunflower._server_event("shutdown", "before")
        await sunflower._server_event("shutdown", "after")
