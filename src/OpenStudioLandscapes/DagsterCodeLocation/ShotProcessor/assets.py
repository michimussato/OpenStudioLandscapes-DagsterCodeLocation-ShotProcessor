import json
import pathlib
import shlex
import shutil
from typing import Generator, Any, Dict, List
# from collections import namedtuple

from dagster import (
# AssetIn,
AssetKey,
asset,
AssetMaterialization,
AssetExecutionContext,
# OpExecutionContext,
Output,
MetadataValue,
)

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import ASSET_HEADER_JOB_PROCESSOR
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.submit_jobs import ASSET_HEADER_JOB_PROCESSOR_DEADLINE
# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs.job_base import Resolution


# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import run_shot_processor
from OpenStudioLandscapes.DagsterCodeLocation.StreamingProcess import submit_cmds


@asset(
    **ASSET_HEADER_JOB_PROCESSOR,
    deps=[
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "job_title"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_filename"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_start_absolute"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_end_absolute"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "submit_job"]),
    ],
    # ins={
    #     "CONFIG": AssetIn(
    #         AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
    #     ),
    # }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        # # batch_name: str,
        # # job_title_str: str,
        # job_title: str,
        # output_format: str,
        # render_output_directory: pathlib.Path,
        # render_output_filename: Dict,
        # frame_start_absolute: int,
        # frame_end_absolute: int,
        # # frames: str,
        # # props: List,
        # # job_model: JobBase,
        # CONFIG: DefaultConstants,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    job_title: str
    output_format: str
    render_output_directory: pathlib.Path
    render_output_filename: Dict
    frame_start_absolute: int
    frame_end_absolute: int
    CONFIG: DefaultConstants

    tasks: List[List] = [
        [
            "ls",
            "-al",
            "/etc",
        ],
        [
            "ls",
            "-al",
            "/",
        ],
    ]

    log_records: List[str] = submit_cmds(
        context=context,
        cmds=tasks,
    )

    yield Output(log_records)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(f"```json\n{json.dumps(log_records)}\n```"),
        }
    )