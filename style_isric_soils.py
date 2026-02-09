import os
from geo.Geoserver import Geoserver
from dotenv import dotenv_values


class AssociateGpkgStyles:
    def __init__(self, **kwargs):
        self.env = dict(dotenv_values())
        self.geo = Geoserver(
            self.env.get("GEOSERVER_URL"),
            username=self.env.get("GEOSERVER_USER"),
            password=self.env.get("GEOSERVER_PASSWORD")
        )
        self.workspaces = eval(self.env.get("ISRIC_GPKG_WORKSPACES"))


    def execute(self):
        for workspace in self.workspaces:
            response_layers = self.geo.get_layers(workspace=workspace).get("layers").get("layer")
            layers = list(map(lambda x: x.get("name"), response_layers))
            workspace_folder = os.path.join(f"datasets/isric_soils/styles/{workspace}")
            for layer in layers:
                sld_path = os.path.join(workspace_folder, f"{layer}.sld")
                has_sld_file = os.path.exists(sld_path)
                if has_sld_file:
                    self.geo.upload_style(name=layer, path=sld_path, workspace=workspace)
                    self.geo.publish_style(layer_name=layer, style_name=layer, workspace=workspace)
                    print(f"✔ Style added to the layer {workspace}: {layer}")
                else:
                    print(f"✘ SLD not available for the layer {workspace}:{layer}")
                    

if __name__ == "__main__":
    AssociateGpkgStyles().execute()
