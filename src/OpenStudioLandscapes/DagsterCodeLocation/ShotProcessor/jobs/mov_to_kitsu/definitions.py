from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets


assets_base = load_assets_from_modules([OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets])


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
