# from typing import List

from dagster import (
    Definitions,
    # load_assets_from_modules,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base import definitions as defs_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.create_text_overlay import definitions as defs_create_text_overlay
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.external_assets import assets_external

# assets_base = load_assets_from_modules([OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets])


defs = Definitions.merge(
    defs_base.defs,
    defs_create_text_overlay.defs,
)
