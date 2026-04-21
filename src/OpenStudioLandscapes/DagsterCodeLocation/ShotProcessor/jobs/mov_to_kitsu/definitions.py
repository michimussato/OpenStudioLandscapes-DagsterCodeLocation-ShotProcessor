from dagster import (
    Definitions,
    load_assets_from_modules,
    AutoMaterializeRule,
    AutoMaterializePolicy,
)


# Assets
import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets
assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.assets],
    auto_materialize_policy=AutoMaterializePolicy.lazy().with_rules(
            AutoMaterializeRule.materialize_on_parent_updated(),
    ),
)


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
