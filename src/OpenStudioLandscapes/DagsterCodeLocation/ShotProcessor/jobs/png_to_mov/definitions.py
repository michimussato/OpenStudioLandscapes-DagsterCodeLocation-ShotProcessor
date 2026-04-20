from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.text_overlay.assets


assets_base = load_assets_from_modules([OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.text_overlay.assets])


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
