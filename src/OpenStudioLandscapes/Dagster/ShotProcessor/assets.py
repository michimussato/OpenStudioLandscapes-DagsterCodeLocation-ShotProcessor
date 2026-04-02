import pathlib
import shlex
import shutil
from typing import Generator, Any, Dict, List
from collections import namedtuple

from dagster import (
AssetIn,
AssetKey,
asset,
AssetMaterialization,
AssetExecutionContext,
OpExecutionContext,
Output,
MetadataValue,
)

from OpenStudioLandscapes.Dagster.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.Dagster.JobProcessor.dagster_job_processor.assets.read_yaml import ASSET_HEADER_JOB_PROCESSOR
# from OpenStudioLandscapes.


@asset(
    **ASSET_HEADER_JOB_PROCESSOR,
    ins={
        "job_title": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "job_title"])
        ),
        "output_format": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"])
        ),
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"])
        ),
        "render_output_filename": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_filename"])
        ),
        "frame_start_absolute": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_start_absolute"])
        ),
        "frame_end_absolute": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_end_absolute"])
        ),
        # "frames": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frames"])
        # ),
        # "props": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "props"])
        # ),
        # "job_model": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "read_job_yaml"])
        # ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
    }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        # batch_name: str,
        # job_title_str: str,
        job_title: str,
        output_format: str,
        render_output_directory: pathlib.Path,
        render_output_filename: Dict,
        frame_start_absolute: int,
        frame_end_absolute: int,
        # frames: str,
        # props: List,
        # job_model: JobBase,
        CONFIG: DefaultConstants,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    proc_exrinfo_pre = []
    for i in range(frame_start_absolute, frame_end_absolute + 1):
        proc_exrinfo_pre.append(
            [
                shutil.which("exrinfo") or "exrinfo",  # avoid empty string if not in PATH
                pathlib.Path(render_output_directory / "raw" / f"{job_title}.{i}.{output_format}").as_posix(),
            ]
        )

    # proc_exrinfo_pre = [
    #     shutil.which("exrinfo") or "exrinfo",  # avoid empty string if not in PATH
    #     render_output_raw.as_posix(),
    # ]

    proc_exrinfo_pre_str = ""
    for cmd in proc_exrinfo_pre:
        proc_exrinfo_pre_str += f"{shlex.join(cmd)};\n"

    # log_records_pre: List[str] = submit_cmds(
    #     context=context,
    #     cmds=proc_exrinfo_pre,
    # )

    borders: int = 100
    Resolution = namedtuple("resolution", ["x", "y"])
    resolution = Resolution(x=960, y=540)

    render_output_oiiotool_src = pathlib.Path(
        render_output_directory / "raw" / render_output_filename["padding_oiiotool"])
    render_output_oiiotool_dst = pathlib.Path(
        render_output_directory / "oiio" / render_output_filename["padding_oiiotool"])

    # Adjust dataWindow
    # oiiotool "${BASE_DIR}/raw/sh030_001.%04d.exr" --origin ${ORIGIN} --fullsize ${FULLSIZE} --create-dir -o "${BASE_DIR}/oiio/sh030_001.%04d.exr"
    proc_oiiotool_expand_data_region = [
        shutil.which("oiiotool") or "oiiotool",  # avoid empty string if not in PATH
        render_output_oiiotool_src.as_posix(),
        "--origin", f"0+{borders}",
        "--fullsize", f"{resolution.x}x{2 * borders + resolution.y}",
        "--create-dir",
        "-o", render_output_oiiotool_dst.as_posix()
    ]

    # render_output_oiio = pathlib.Path(render_output_directory / "oiio" / render_output_filename["padding_bash_expansion"])
    #
    # # exrinfo "${BASE_DIR}/oiio/sh030_001.${START_F}.exr"
    # proc_exrinfo_post = [
    #     shutil.which("exrinfo") or "exrinfo",  # avoid empty string if not in PATH
    #     render_output_oiio.as_posix(),
    # ]

    proc_exrinfo_post = []
    for i in range(frame_start_absolute, frame_end_absolute + 1):
        proc_exrinfo_post.append(
            [
                shutil.which("exrinfo") or "exrinfo",  # avoid empty string if not in PATH
                pathlib.Path(render_output_directory / "oiio" / f"{job_title}.{i}.{output_format}").as_posix(),
            ]
        )

    # proc_exrinfo_pre = [
    #     shutil.which("exrinfo") or "exrinfo",  # avoid empty string if not in PATH
    #     render_output_raw.as_posix(),
    # ]

    proc_exrinfo_post_str = ""
    for cmd in proc_exrinfo_post:
        proc_exrinfo_post_str += f"{shlex.join(cmd)};\n"

    # log_records_pre: List[str] = submit_cmds(
    #     context=context,
    #

    cmds_oiio: List = [
        *proc_exrinfo_pre,
        proc_oiiotool_expand_data_region,
        *proc_exrinfo_post,
    ]

    # log_records: List[str] = submit_cmds(
    #     context=context,
    #     cmds=cmds_oiio,
    # )

    yield Output(cmds_oiio)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(cmds_oiio),
            "proc_exrinfo_pre_str": MetadataValue.md(f"```\n{proc_exrinfo_pre_str}\n```"),
            "proc_oiiotool_expand_data_region": MetadataValue.path(shlex.join(proc_oiiotool_expand_data_region)),
            "proc_exrinfo_post_str": MetadataValue.md(f"```\n{proc_exrinfo_post_str}\n```"),
            # "proc_exrinfo_pre": MetadataValue.path(shlex.join(proc_exrinfo_pre)),
            # "proc_exrinfo_post": MetadataValue.path(shlex.join(proc_exrinfo_post)),
            # "log_records": MetadataValue.md(f"```shell\n{log_records}\n```"),
        }
    )