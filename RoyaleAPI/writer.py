import datetime
from http import client
import os
from tempfile import NamedTemporaryFile, SpooledTemporaryFile
import tempfile
from typing import List, Union
import json
import boto3
from api import *
from ingestors import *




class DataTypeNotSuportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)

class DataWriter:
    def __init__(self, api: str, sub_type: str) -> None:
        self.api = api
        self.sub_type = sub_type
        self.filename = f"{self.api}/{self.sub_type}/{datetime.datetime.now()}.json" 
    
    def _write_row(self, row: str) -> None:
        os.makedirs(os.path.dirname(self.filename),exist_ok=True)
        with open(self.filename, "a") as f:
            for result in row:
                f.write(result)


    def _write_to_file(self, data: Union[List, dict]):
        if isinstance(data,dict): #verifica se data esta é uma instancia de dicionario
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data, List):
            for element in data:
                self._write_row(json.dumps(element) + "\n") 
        else:
            raise DataTypeNotSuportedForIngestionException(data)

    def write(self, data: Union[List, dict]):
        self._write_to_file(data=data)


class S3PlayerWriter(DataWriter):
    def __init__(self, api: str, sub_type: str, tag: str) -> None: 
        super().__init__(api, sub_type)
        self.tag = tag
        self.tempfile = NamedTemporaryFile()
        self.client = boto3.client("s3")
        self.key = f"APIRoyale/{self.api}/sub_type={self.sub_type}/extracted_at={datetime.datetime.now().date()}/{self.tag}_{datetime.datetime.now()}.json"

    def _write_row(self, row: str) -> None:
        with open(self.tempfile.name, "a") as f:
            for result in row:
                f.write(result)

    def write(self, data: Union[List, dict]):
        self._write_to_file(data=data)
        self._write_file_to_s3()

    def _write_file_to_s3(self):
        self.client.put_object(
            Body = self.tempfile,
            Bucket = "apiroyale-raw",
            Key = self.key
        )
