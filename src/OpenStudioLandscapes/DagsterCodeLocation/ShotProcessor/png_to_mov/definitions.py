from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetSpec,
    AssetKey,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.png_to_mov import assets  # noqa: TID252
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.png_to_mov.assets import (
    # ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
    ASSET_HEADER_JOB_PROCESSOR_READER,
    ASSET_HEADER_JOB_PROCESSOR,
    # ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU,
    # ASSET_HEADER_OIIO_PROCESSOR,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets import (
ASSET_HEADER_OIIO_PROCESSOR
)

all_assets = load_assets_from_modules([assets])


# External Assets
external_assets: List[AssetSpec] = []

# [x] image_sequence
# [x] raw_to_oiio
# [x] plugin_info_model
render_version_directory = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "render_version_directory"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.render_version_directory`.",
)
external_assets.append(render_version_directory)

read_job_yaml = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"],
            "read_job_yaml"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR_READER["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.read_job_yaml`.",
)
external_assets.append(read_job_yaml)

# [x] image_sequence
# [x] raw_to_oiio
# [x] plugin_info_model
render_output_directory = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "render_output_directory"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.render_output_directory`.",
)
external_assets.append(render_output_directory)

# [ ] image_sequence
# [ ] raw_to_oiio
# [x] job_info
batch_name = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "batch_name"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.batch_name`.",
)
external_assets.append(batch_name)

# [ ] image_sequence
# [ ] raw_to_oiio
# [x] job_info
job_title_str = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "job_title_str"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.job_title_str`.",
)
external_assets.append(job_title_str)

# [x] image_sequence
output_format = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "output_format"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.output_format`.",
)
external_assets.append(output_format)

# [x] image_sequence
# [x] raw_to_oiio
version = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "version"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.version`.",
)
external_assets.append(version)

# [x] image_sequence
CONFIG = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "CONFIG"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.CONFIG`.",
)
external_assets.append(CONFIG)

CONFIG_OIIO = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_OIIO_PROCESSOR["key_prefix"],
            "CONFIG_OIIO"
        ],
    ),
    group_name=ASSET_HEADER_OIIO_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets.CONFIG_OIIO` "
                "`CONFIG_OIIO` AssetOut.",
)
external_assets.append(CONFIG_OIIO)


defs = Definitions(
    assets=[
        *all_assets,
        # submit_job,
        *external_assets,
    ],
)
