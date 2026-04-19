from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.assets


assets_base = load_assets_from_modules([OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.assets])


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
