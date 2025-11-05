from sanic import HTTPResponse, json

from sunflower.infrastructure.sanic.app import SunflowerRequest


async def not_found(request: SunflowerRequest, exception: Exception) -> HTTPResponse:
    return json(
        {
            "message": str(exception),
        },
        status=404,
    )
