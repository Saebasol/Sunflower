from sanic.blueprints import Blueprint

from sunflower.adapters.endpoint.api import api_endpoint
from sunflower.adapters.endpoint.root import root_endpoint

endpoint = Blueprint.group(api_endpoint, root_endpoint)
