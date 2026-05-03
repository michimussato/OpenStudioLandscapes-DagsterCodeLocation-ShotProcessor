from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
    AutoMaterializeRule,
    AutoMaterializePolicy,
)

import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_to_png.assets


assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_to_png.assets],
    auto_materialize_policy=AutoMaterializePolicy.lazy().with_rules(
            AutoMaterializeRule.materialize_on_parent_updated(),
    ),
)


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
