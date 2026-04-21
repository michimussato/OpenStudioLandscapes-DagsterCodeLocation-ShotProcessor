from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
    AutoMaterializePolicy,
    AutoMaterializeRule,
)

import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.assets


assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.assets],
    auto_materialize_policy=AutoMaterializePolicy.lazy().with_rules(
            AutoMaterializeRule.materialize_on_parent_updated(),
    ),
)


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
