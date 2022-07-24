import requests
import logging
from abc import ABC, abstractmethod
import ratelimit
from backoff import on_exception, expo
import urllib.parse

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RoyaleAPI(ABC):
    def __init__(self) -> None:
        self.base_endpoint = "https://api.clashroyale.com/v1"

    @property
    def _get_credential(self) -> str:
        with open("my_key.txt") as f:
            return f.read().rstrip("\n")

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass

    # return f"{self.base_endpoint}/%239C0CCLYPP/battlelog"
    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from: {endpoint}")
        response = requests.get(
            endpoint, {"Authorization": f"Bearer {self._get_credential}"}
        )
        response.raise_for_status()
        return response.json()


# print(ApiRoyale().get_data())


class PlayersAPI(RoyaleAPI):
    type = "players"
    # sub_types = battlelog, upcomingchests, players

    def _get_endpoint(self, tag: str, sub_type: str = None) -> str:
        tag = urllib.parse.quote_plus(tag)
        sub_type = sub_type
        if sub_type == "players":
            return f"{self.base_endpoint}/{self.type}/{tag}"
        else:
            return f"{self.base_endpoint}/{self.type}/{tag}/{sub_type}"
            # return f"{self.base_endpoint}/{self.type}/{tag}/upcomingchests"


class LocationsAPI(RoyaleAPI):
    type = "locations"
    # sub_types = clans or players or clanwars

    def _get_endpoint(self, loc_id: int = None, sub_type: str = None) -> str:
        # sourcery skip: remove-redundant-if
        loc_id = loc_id
        sub_type = sub_type

        if not loc_id and not sub_type:
            return f"{self.base_endpoint}/{self.type}"
        elif loc_id and not sub_type:
            return f"{self.base_endpoint}/{self.type}/{loc_id}"
        else:
            return f"{self.base_endpoint}/{self.type}/{loc_id}/rankings/{sub_type}"
