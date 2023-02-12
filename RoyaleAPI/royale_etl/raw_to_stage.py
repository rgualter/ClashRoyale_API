#%%
import json
import logging
import pandas as pd
import os
import glob
import datetime
from abc import abstractmethod
from pathlib import Path


class FileLoader:
    def __init__(self):
        self.path = Path("data")
        self.files = self.path.glob("*.json")
        self.output_dir = self.path / "output"

    def load_files(self, files):
        data = pd.DataFrame()
        self.transformer = DataTransformer()
        for file in files:
            file_data = [json.loads(line) for line in open(file)]
            df_team = self.transformer.transform_data(file_data, "team", file)
            df_opponent = self.transformer.transform_data(file_data, "opponent", file)
            file_data = pd.concat([df_team, df_opponent])
            data = pd.concat([data, file_data], ignore_index=True)
        return data


class DataTransformer:
    def __init__(self):
        self.load = FileLoader()

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

    # def rename_columns(self,df,type):
    #        for t in type:
    #                df.rename(
    #                columns={
    #                    "m." + t + ".startingTrophies": "m.startingTrophies",
    #                    "m." + t + ".trophyChange": "m.trophyChange",
    #                    "m." + t + ".crowns": "m.crowns",
    #                    "m." + t + ".kingTowerHitPoints": "m.kingTowerHitPoints",
    #                    "m." + t + ".clan.tag": "m.clan.tag",
    #                    "m." + t + ".clan.name": "m.clan.name",
    #                    "m." + t + ".tag": "m.team.tag",
    #                    "m." + t + ".name": "m.team.name"
    #                },
    #                inplace=True,
    #            )
    #        return df

    # def rename_columns(self,df,type):
    #    mappings = {f"m.{type}.startingTrophies": "m.startingTrophies",
    #                f"m.{type}.trophyChange": "m.trophyChange",
    #                f"m.{type}.crowns": "m.crowns",
    #                f"m.{type}.kingTowerHitPoints": "m.kingTowerHitPoints",
    #                f"m.{type}.clan.tag": "m.clan.tag",
    #                f"m.{type}.clan.name": "m.clan.name",
    #                f"m.{type}.tag": "m.team.tag",
    #                f"m.{type}.name": "m.team.name"}
    #    for _ in type:
    #        df.rename(columns=mappings, inplace=True)
    #    return df

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
        self.transformer = DataTransformer()
        self.load = FileLoader()
        self.data = self.load.load_files(self.load.files)
        self.output_dir = self.load.output_dir

    def write_to_csv(self):
        self.data.to_csv(
            f"{self.output_dir}/{datetime.datetime.now()}.csv", header=True
        )

#%%
if __name__ == "__main__":
    file_writer = DataWriter()
    file_writer.write_to_csv()
