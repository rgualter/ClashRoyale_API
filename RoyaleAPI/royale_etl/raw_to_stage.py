import io
import json
import pandas as pd
import os
import datetime
from pathlib import Path
import boto3
import re
from abc import ABC, abstractmethod
from tempfile import NamedTemporaryFile, SpooledTemporaryFile
from io import StringIO, BytesIO
import csv
import logging
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class DataLoader(ABC):
    def __init__(self):
        self.transformer = DataTransformer()

    @abstractmethod
    def load_data(self):
        pass


class FileLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.path = Path("data")
        self.files = self.path.glob("*.json")

    def load_data(self):
        data = pd.DataFrame()
        files = self.files
        transformer = self.transformer

        for file in files:
            file_data = [json.loads(line) for line in open(file)]
            df_team = transformer.transform_data(file_data, "team", file)
            df_opponent = transformer.transform_data(file_data, "opponent", file)
            file_data = pd.concat([df_team, df_opponent])
            data = pd.concat([data, file_data], ignore_index=True)
        return data


class S3Loader(DataLoader):
    def __init__(self):
        super().__init__()
        self.now = datetime.datetime.now()
        self.date_str = self.now.strftime("%Y-%m-%d")
        self.date_pattern = r".*\d{4}-\d{2}-\d{2}.*\.json"
        self.prefix = (
            f"APIRoyale/players/sub_type=battlelog/extracted_at={self.date_str}/"
        )
        self.bucket_name = "apiroyale-raw"
    

    def read_json_from_s3(self, bucket_name, file_name):
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        json_file = response["Body"].read().decode("utf-8").splitlines()
        return [json.loads(line) for line in json_file]

    def load_data(self):  # sourcery skip: raise-specific-error
        data = pd.DataFrame()
        bucket_name = self.bucket_name
        prefix = self.prefix
        date_pattern = self.date_pattern
        transformer = self.transformer

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        all_files = response.get("Contents", [])
        json_files = [f for f in all_files if re.match(date_pattern, f["Key"])]

        if not json_files:
            msg = f"No data found for date {self.date_str} in bucket {bucket_name}"
            logging.error(msg)
            raise Exception(msg)
        
        logger.info(f"Loading data from: {bucket_name}")

        for f in json_files:
            json_data = self.read_json_from_s3(bucket_name, f["Key"])
            df_team = transformer.transform_data(json_data, "team", f["Key"])
            df_opponent = transformer.transform_data(json_data, "opponent", f["Key"])
            file_data = pd.concat([df_team, df_opponent])
            data = pd.concat([data, file_data], ignore_index=True)

        return data


class DataTransformer:
    def __init__(self):
        pass

    def get_FilenameColumns(self, files):
        raw_file_name = os.path.basename(files)
        file_name = raw_file_name.split(".")[0]
        principal_user = file_name.split("_")[0]
        return raw_file_name, principal_user

    def split_columns(self, df):
        columns = [
            ("m.arena", "id", "m.arena.id"),
            ("m.arena", "name", "m.arena.name"),
            ("m.gameMode", "id", "m.gameMode.id"),
            ("m.gameMode", "name", "m.gameMode.name"),
        ]
        for col1, col2, new_col in columns:
            df.loc[:, new_col] = df[col1].apply(pd.Series).loc[:, col2]

    def split_list_column(self, df, type):
        col = f"m.{type}.princessTowersHitPoints"
        indices = [0, 1]
        new_cols = ["princessTowersHitPoints1", "princessTowersHitPoints2"]

        for idx, new_col in zip(indices, new_cols):
            df[new_col] = df[col].str[idx]
        return df

    def create_id_column(self, df):
        dep_id_cols = ["m.battleTime", "principal_user"]
        df["file_battle_id"] = (
            df[dep_id_cols].ne(df[dep_id_cols].shift()).any(axis=1).cumsum()
        )

    def create_meta_columns(self, df, file):
        raw_file_name, principal_user = self.get_FilenameColumns(file)
        df[["raw_layer_filename", "principal_user"]] = (
            raw_file_name,
            principal_user,
        )

    def delete_columns(self, df, type):
        df = df.drop(
            columns=["m.gameMode", "m.arena", f"m.{type}.princessTowersHitPoints"]
        )
        return df

    def rename_columns(self, df):
        df.rename(
            columns={
                "m.team.startingTrophies": "m.startingTrophies",
                "m.team.trophyChange": "m.trophyChange",
                "m.team.crowns": "m.crowns",
                "m.team.kingTowerHitPoints": "m.kingTowerHitPoints",
                "m.team.clan.tag": "m.clan.tag",
                "m.team.clan.name": "m.clan.name",
                "m.team.tag": "m.team.tag",
                "m.team.name": "m.team.name",
                "m.opponent.startingTrophies": "m.startingTrophies",
                "m.opponent.trophyChange": "m.trophyChange",
                "m.opponent.crowns": "m.crowns",
                "m.opponent.kingTowerHitPoints": "m.kingTowerHitPoints",
                "m.opponent.clan.tag": "m.clan.tag",
                "m.opponent.clan.name": "m.clan.name",
                "m.opponent.tag": "m.team.tag",
                "m.opponent.name": "m.team.name",
            },
            inplace=True,
        )

    def _json_normalize(self, data, type):
        # sourcery skip: inline-immediately-returned-variable
        df = pd.json_normalize(
            data,
            record_path=[type, "cards"],
            meta=[
                "type",
                "battleTime",
                "isLadderTournament",
                "deckSelection",
                ["arena"],
                ["gameMode"],
                [type, "clan", "tag"],
                [type, "clan", "name"],
                [type, "tag"],
                [type, "name"],
                [type, "startingTrophies"],
                [type, "trophyChange"],
                [type, "crowns"],
                [type, "kingTowerHitPoints"],
                [type, "princessTowersHitPoints"],
            ],
            errors="ignore",
            record_prefix="r.",
            meta_prefix="m.",
        ).reset_index()
        return df

    def transform_data(self, data, type, file):
        self.data = data
        self.file = file

        logger.info(f"Transforming data: Type: {type} from: {file}")

        df = self._json_normalize(data, type)
        df["team_or_opponent"] = type
        self.split_columns(df)
        self.split_list_column(df, type)
        self.create_meta_columns(df, file)
        self.create_id_column(df)
        self.rename_columns(df)
        df = self.delete_columns(df, type)
        return df


class DataWriter:
    def __init__(self):
        self.path = Path("data")
        self.output_dir = self.path / "output"

    def write_to_csv(self, data):
        data.to_csv(f"{self.output_dir}/{datetime.datetime.now()}.csv", header=True)


class S3DataWriter(DataWriter):
    def __init__(self, data, format):
        super().__init__()
        self.now = datetime.datetime.now()
        self.date_str = self.now.date()  # strftime("%Y-%m-%d")
        self.client = boto3.client("s3")
        self.bucket_name = "apiroyale-stage"
        self.format = f"{format}"
        self.key = f"APIRoyale/players/sub_type=battlelog/transformed_at={self.date_str}/{self.now}.{self.format}"
        self.data = data

    def write_to_csv_s3(self):
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)
        body = csv_buffer.getvalue()
        logger.info(f"Writing csv data to: Bucket:{self.bucket_name}/{self.key}")
        self.client.put_object(Bucket=self.bucket_name, Key=self.key, Body=body)

    def write_parquet_to_s3(self):
        buffer = BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        buffer.seek(0)
        logger.info(f"Writing parquet data to: Bucket:{self.bucket_name}/{self.key}")
        self.client.upload_fileobj(buffer, self.bucket_name, self.key)

    def write(self):
        self.write_to_csv_s3(data)
        self.write_to_csv(data)
        self.write_parquet_to_s3(data)


if __name__ == "__main__":
    # Load data from S3 bucket and apply DataTransformer ETL
    loader = S3Loader()
    data = loader.load_data()
    # Create S3 data writer and Write transformed data to CSV file
    data_writer = S3DataWriter(data, "parquet")
    data_writer.write_parquet_to_s3()
