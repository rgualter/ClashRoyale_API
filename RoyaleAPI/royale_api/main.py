
from schedule import repeat, every, run_pending
from royale_api.ingestors import PlayersApiIngestor
from royale_api.writer import *


if __name__ == "__main__":
    PlayersApiIngestor = PlayersApiIngestor(
        tag=["#9C0CCLYPP", "#YLY8GJ0LY"],
        writer=S3PlayerWriter,
        sub_type=["battlelog", "upcomingchests", "players"],
    )
    PlayersApiIngestor.ingest()
