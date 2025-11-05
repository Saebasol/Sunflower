from typing import Optional

from sanic import file
from sanic.blueprints import Blueprint
from sanic.response import HTTPResponse
from sanic.views import HTTPMethodView

from sunflower.infrastructure.sanic.app import SunflowerRequest

root_endpoint = Blueprint("root", url_prefix="/")


class SunflowerRootView(HTTPMethodView):
    async def get(self, request: SunflowerRequest) -> Optional[HTTPResponse]:
        return await file(
            "./sunflower/adapters/endpoint/dashboard.html",
            mime_type="text/html",
        )


root_endpoint.add_route(SunflowerRootView.as_view(), "/")
