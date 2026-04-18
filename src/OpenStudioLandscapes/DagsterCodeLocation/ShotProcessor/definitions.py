# This definition file is the main entry point
# for the ShotProcessor Code Location in
# OpenStudioLandscapes-Dagster

from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base import definitions as defs_base
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base._definitions_with_upstream_specs import assets_external as base_assets_external
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.create_text_overlay import definitions as defs_create_text_overlay
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.create_text_overlay._definitions_with_upstream_specs import assets_external as create_text_overlay_assets_external


defs = Definitions.merge(
    defs_base.defs,
    # defs_create_text_overlay.defs,
)
