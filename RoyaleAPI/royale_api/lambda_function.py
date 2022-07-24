from royale_api.ingestors import PlayersApiIngestor
from royale_api.writer import *
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    logger.info(f"{event}")
    logger.info(f"{context}")
    PlayersApiIngestor(
        tag=["#9C0CCLYPP", "#YLY8GJ0LY"],
        writer=S3PlayerWriter,
        sub_type=["battlelog", "upcomingchests", "players"],
    ).ingest()
