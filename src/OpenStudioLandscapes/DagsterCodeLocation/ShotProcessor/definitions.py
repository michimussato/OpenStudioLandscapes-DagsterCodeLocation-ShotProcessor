from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetSpec,
    AssetKey,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor import assets  # noqa: TID252
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets import (
    ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
    ASSET_HEADER_JOB_PROCESSOR_READER,
    ASSET_HEADER_JOB_PROCESSOR,
    ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU,
    # ASSET_HEADER_OIIO_PROCESSOR,
)

all_assets = load_assets_from_modules([assets])


# External Assets
external_assets: List[AssetSpec] = []

# # [x] image_sequence
# # [x] raw_to_oiio
# submit_job = AssetSpec(
#     key=AssetKey(
#         [
#             *ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"],
#             "submit_job"
#         ],
#     ),
#     group_name=ASSET_HEADER_JOB_SUBMITTER_DEADLINE["group_name"],
#     description="Entry point for "
#                 "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.submit_jobs.submit_job`.",
# )
# external_assets.append(submit_job)

# [x] image_sequence
# [x] raw_to_oiio
render_version_directory = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR["key_prefix"],
            "render_version_directory"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.render_output_directory`.",
)
external_assets.append(render_version_directory)

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

# submit_job = AssetSpec(
#     key=AssetKey(
#         [
#             *ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"],
#             "submit_job"
#         ],
#     ),
#     group_name=ASSET_HEADER_JOB_SUBMITTER_DEADLINE["group_name"],
#     description="AssetDefinition from `CodeLocation1.assets`. "
#                 "Description from AssetSpec in "
#                 "`Base.definitions`.",
# )

# [ ] image_sequence
# [x] raw_to_oiio
get_kitsu_task_dict = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"],
            "get_kitsu_task_dict"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.get_kitsu_task_dict`.",
)
external_assets.append(get_kitsu_task_dict)

# [ ] image_sequence
# [ ] raw_to_oiio
# [x] submit_request_png_to_mov
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

# [ ] image_sequence
# [ ] raw_to_oiio
# [ ] submit_request_png_to_mov
# [x] payload_png_to_mov
job_info_model = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"],
            "job_info_model"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR_DEADLINE["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.job_info_file` "
                "`job_info_model` AssetOut.",
)
external_assets.append(job_info_model)

# [ ] image_sequence
# [ ] raw_to_oiio
# [ ] submit_request_png_to_mov
# [x] payload_png_to_mov
plugin_info_model = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"],
            "plugin_info_model"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR_DEADLINE["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.plugin_info_file` "
                "`plugin_info_model` AssetOut.",
)
external_assets.append(plugin_info_model)

# [ ] image_sequence
# [ ] raw_to_oiio
# [ ] submit_request_png_to_mov
# [ ] payload_png_to_mov
# [x] png_to_mov
job_id_raw = AssetSpec(
    key=AssetKey(
        [
            *ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"],
            "job_id_raw"
        ],
    ),
    group_name=ASSET_HEADER_JOB_PROCESSOR_DEADLINE["group_name"],
    description="Entry point for "
                "`OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml.plugin_info_file` "
                "`job_id_raw` AssetOut.",
)
external_assets.append(job_id_raw)


defs = Definitions(
    assets=[
        *all_assets,
        # submit_job,
        *external_assets,
    ],
)
