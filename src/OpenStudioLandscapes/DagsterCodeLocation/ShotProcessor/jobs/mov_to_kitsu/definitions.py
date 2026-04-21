from dagster import (
    Definitions,
    load_assets_from_modules,
)


# Assets
import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets
assets_base = load_assets_from_modules([OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets])


# Resources
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.resources.kitsu import KitsuResource
resources = {
    "kitsu_resource": KitsuResource(),
}


defs = Definitions(
    assets=[
        *assets_base,
    ],
    resources=resources,
)
