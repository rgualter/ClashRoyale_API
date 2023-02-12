from abc import ABC, abstractmethod
from api import PlayersAPI


class DataIngestor(ABC):
    def __init__(self, writer, tag: list[str], sub_type: list[str]) -> None:
        self.writer = writer
        self.tag = tag
        self.sub_type = sub_type

    @abstractmethod
    def ingest(self) -> None:
        pass


class PlayersApiIngestor(DataIngestor):
    def ingest(self) -> None:
        for sub_type in self.sub_type:
            for my_tag in self.tag:
                api = PlayersAPI()
                my_tag = my_tag
                data = api.get_data(tag=my_tag, sub_type=sub_type)
                self.writer(sub_type=sub_type, api=api.type, tag=my_tag).write(data)
