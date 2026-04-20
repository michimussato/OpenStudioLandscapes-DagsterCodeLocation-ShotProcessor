from dagster import (
    Definitions,
)

# from OpenStudioLandscapes.engine.env.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    output_format,
    CONFIG,
    render_output_directory,
    fps,
)

assets_external = []
assets_external.extend(output_format.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(render_output_directory.specs)
assets_external.extend(fps.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
