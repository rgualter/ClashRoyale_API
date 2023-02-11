
from schedule import repeat, every, run_pending
from ingestors import PlayersApiIngestor
from writer import *


if __name__ == "__main__":
    PlayersApiIngestor = PlayersApiIngestor(
        tag=["#9C0CCLYPP", "#YLY8GJ0LY"],
        writer=S3PlayerWriter,
        sub_type=["battlelog", "upcomingchests", "players"],
    )
    PlayersApiIngestor.ingest()
