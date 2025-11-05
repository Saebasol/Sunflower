from sanic.blueprints import Blueprint

from sunflower.adapters.endpoint.api.status import status_endpoint

api_endpoint = Blueprint.group(
    status_endpoint,
    url_prefix="/api",
)
