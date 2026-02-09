import os
import pandas as pd
from geo.Geoserver import Geoserver
from dotenv import dotenv_values


class PublishSoilsGpkg:
    def __init__(self, **kwargs):
        self.env = kwargs.get("env")
        self.geo = Geoserver(
            self.env.get("GEOSERVER_URL"),
            username=self.env.get("GEOSERVER_USER"),
            password=self.env.get("GEOSERVER_PASSWORD"),
        )
        self.workspace = kwargs.get("workspace")
        self.index_df = pd.read_csv("./isric_gpkg_layer_index.csv")


    def execute(self):
        df = self.index_df[self.index_df["workspace"] == self.workspace]
        row_count = len(df.index)
        for _, gpkg_item in df.iterrows():
            try:
                self.geo.create_gpkg_datastore(
                    workspace=self.workspace,
                    store_name=gpkg_item.get("store"),
                    path=gpkg_item.get("file_path"),
                )
                print(f"✔ Layer published -{gpkg_item.get('sno')}/{row_count}: {gpkg_item.get('workspace')}:{gpkg_item.get('store')}")
            except Exception as e:
                print(f"✘ Unable to publish the GPKG ISRIC soils store/layer - {gpkg_item.get('store')} \t {str(e)}")


if __name__ == "__main__":
    env = dict(dotenv_values())
    workspaces = eval(env.get("ISRIC_GPKG_WORKSPACES"))
    for workspace in workspaces:
        PublishSoilsGpkg(env=env, workspace=workspace).execute()