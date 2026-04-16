import json
import os
import pathlib
import requests
import re
from typing import Generator, Any, Dict, List, Union

import yaml

from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    asset,
    multi_asset,
    AssetMaterialization,
    AssetExecutionContext,
    Output,
    MetadataValue,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    ASSET_HEADER_JOB_PROCESSOR,
    ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU,
    ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
    ASSET_HEADER_JOB_PROCESSOR_READER,
)

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs.job_base import JobBase
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs import models_submission

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import process_image, create_buf_from_raw
from OpenStudioLandscapes.DagsterCodeLocation.StreamingProcess import submit_cmds

# Asset data across code locations:
# - [SourceAsset](https://stackoverflow.com/q/79780791)
# - [AssetSpec](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/external-assets)
# - [Asset obervations](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/asset-observations)


GROUP_OIIO_PROCESSOR = "OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor"
# KEY_CONSTANTS_DEFAULT = [GROUP_CONSTANTS_DEFAULT, "Constants"]
KEY_OIIO_PROCESSOR = [GROUP_OIIO_PROCESSOR]

ASSET_HEADER_OIIO_PROCESSOR = {
    "group_name": GROUP_OIIO_PROCESSOR,
    "key_prefix": KEY_OIIO_PROCESSOR,
}


@multi_asset(
    # **ASSET_HEADER_OIIO_PROCESSOR,
    outs={
        # "env": AssetOut.from_spec(env_spec),
        "CONFIG_OIIO": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            # dagster_type=ConfigOIIO,
            description="Todo",
        ),
    },
)
def CONFIG_OIIO(
        context: AssetExecutionContext,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    config_oiio: ConfigOIIO = ConfigOIIO()

    ###############
    # CONFIG_OIIO #
    ###############

    output_name = "CONFIG_OIIO"

    yield Output(
        output_name=output_name,
        value=config_oiio,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(json.loads(config_oiio.model_dump_json(fallback=str, indent=2)))}\n```"
            ),
        },
    )


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    # deps=[
    #     AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    # ],
    ins={
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        "output_format": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
    }
)
def image_sequence_raw(
        context: AssetExecutionContext,
        render_version_directory: pathlib.Path,
        output_format: str,
        version: str,
        CONFIG: DefaultConstants,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    sequence_dir: pathlib.Path = render_version_directory.joinpath(version, CONFIG.RENDER_RAW_OUT)

    ret = []

    for root, dirs, files in os.walk(sequence_dir):
        # sort:
        # - [](https://stackoverflow.com/a/18282401)
        for dir_ in sorted(dirs):
            context.log.debug("Processing directory %s", dir_)
        for file_ in sorted(files):
            filepath = pathlib.Path(root, file_)
            context.log.debug("Processing file: %s", filepath)
            if file_.endswith(f".{output_format}"):
                context.log.debug("Appending file to list: %s", filepath)
                ret.append(filepath)
            else:
                context.log.info(f"Skipping file because it does not have extension `.{output_format}`: %s", filepath)

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(
                f"```json\n{json.dumps(ret, indent=2, default=str)}\n```"
            ),
        }
    )


@multi_asset(
    outs={
        "raw_to_oiio": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            dagster_type=List[Dict],
            description="Todo",
        ),
    },
    # deps=[
    #     AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    # ],
    ins={
        "get_kitsu_task_dict": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"], "get_kitsu_task_dict"]),
        ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        "image_sequence_raw": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "image_sequence_raw"]),
        ),
        "CONFIG_OIIO": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "CONFIG_OIIO"]),
        ),
    }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        image_sequence_raw: List[pathlib.Path],
        version: str,
        render_version_directory: pathlib.Path,
        get_kitsu_task_dict: Dict,
        CONFIG_OIIO: ConfigOIIO,
) -> Generator[Union[Output[List[Dict]]] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    results = []

    output_dir: pathlib.Path = render_version_directory.joinpath(
        version,
        "oiio",
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    for image_ in image_sequence_raw:
        context.log.debug("Processing image %s", image_)

        # Frame number based on image name (image.0123.png)
        f_no_ = re.findall(
            r"\.[0-9]+\.",
            image_.name
        )

        if bool(f_no_):
            f_no = int(f_no_[-1].replace(".", ""))
        else:
            f_no = 0

        context.log.debug(f"Frame number: {f_no}")

        raw_buf, raw_spec = create_buf_from_raw(
            raw=image_
        )

        processed_result = process_image(
            # raw_buf=raw_buf,
            raw_spec=raw_spec,
            context=context,
            image_filepath=image_,
            frame_number=f_no,
            kitsu_task_dict=get_kitsu_task_dict,
            CONFIG_OIIO=CONFIG_OIIO,
            version=version,
            output_dir=output_dir,
            create_exr_from_raw_with_custom_metadata=True,
            create_text_overlay=True,
            create_handle_overlay=True,
            create_png=True,
        )
        context.log.debug(f"{processed_result = }")
        results.append(processed_result)

    ###############
    # raw_to_oiio #
    ###############

    output_name = "raw_to_oiio"

    yield Output(
        output_name=output_name,
        value=results,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.md(
                f"```json\n{json.dumps(results, indent=2, default=str)}\n```"
            ),
        }
    )


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "job_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "job_info_model"]),
        ),
        "plugin_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "plugin_info_model"]),
        ),
    },
)
def payload_png_to_mov(
        context: AssetExecutionContext,
        CONFIG: DefaultConstants,
        job_info_model: models_submission.JobInfo,
        plugin_info_model: models_submission.CommandLinePluginInfo,
) -> Generator[Output[Dict] | AssetMaterialization | Any, Any, None]:

    """
    Before:
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/037/4_1197-1254_4/combined_dict.json"

    After
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/045/4_0997-1104_4/combined_dict.json"
    """

    headers = {
        "Content-Type": "application/json",
        "Accept-Charset": "UTF-8",
    }

    context.log.debug(f"{headers = }")

    # https://docs.thinkboxsoftware.com/products/deadline/10.2/1_User%20Manual/manual/rest-jobs.html#submit-job
    payload_raw = {
        "JobInfo": json.loads(job_info_model.model_dump_json(indent=2, fallback=str)),
        "PluginInfo": json.loads(plugin_info_model.model_dump_json(indent=2, fallback=str)),
        "IdOnly": False,
        "AuxFiles": [],
    }

    context.log.debug(f"{payload_raw = }")

    yield Output(payload_raw)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(
                f"```json\n{json.dumps(payload_raw, default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
        }
    )


@multi_asset(
    outs={
        "job_png_to_mov": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            dagster_type=Dict,
            description="",
        ),
        "job_id_png_to_mov": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            dagster_type=str,
            description="",
        ),
    },
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "payload_png_to_mov": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "payload_png_to_mov"]),
        ),
        "job_model": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"], "read_job_yaml"])
        ),
    },
)
def submit_request_png_to_mov(
        context: AssetExecutionContext,
        CONFIG: DefaultConstants,
        payload_png_to_mov: Dict,
        job_model: JobBase,
) -> Generator[Output[requests.Response] | AssetMaterialization | Any, Any, None]:

    """
    Before:
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/037/4_1197-1254_4/combined_dict.json"

    After
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/045/4_0997-1104_4/combined_dict.json"
    """

    headers = {
        "Content-Type": "application/json",
        "Accept-Charset": "UTF-8",
    }

    context.log.debug(f"{headers = }")

    payload = json.dumps(payload_png_to_mov, indent=CONFIG.JSON_INDENT, sort_keys=True, default=str)

    context.log.debug(f"{payload = }")

    context.log.info(f"Sending request to {job_model.deadline_config.rest_api_jobs}...")

    # Requests: data vs. json:
    # - https://stackoverflow.com/a/26685359/2207196
    # - https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
    #   > If you need that header set and you don’t want to encode the dict yourself, you can
    #   > also pass it directly using the json parameter (added in version 2.4.2) and it will
    #   > be encoded automatically
    # -> using `json=` does not serialize as expected (yet). Hence, `data=` and manual.
    request = requests.Request(
        url=job_model.deadline_config.rest_api_jobs,
        method="POST",
        headers=headers,
        # json=payload_raw,
        data=payload,
    )

    context.log.debug(f"{request = }")

    prepared_request = request.prepare()

    context.log.debug(f"{prepared_request = }")

    session = requests.Session()
    response = session.send(prepared_request, verify=False)

    context.log.debug(f"{response = }")
    context.log.debug(f"{response.raw = }")
    context.log.debug(f"{response.status_code = }")
    # context.log.debug(f"{response.content = }")
    context.log.debug(f"{response.text = }")

    output_name = "job_png_to_mov"

    yield Output(
        output_name=output_name,
        value=response.json(),
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(context.asset_key_for_output(output_name).path): MetadataValue.json(payload),
            "headers": MetadataValue.md(
                f"```json\n{json.dumps(headers, default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
            "payload": MetadataValue.md(
                f"```json\n{payload}\n```"
            ),
            "request": MetadataValue.md(
                f"```json\n{json.dumps(request.__dict__, indent=CONFIG.JSON_INDENT, default=str, sort_keys=True)}\n```"
            ),
            "response": MetadataValue.md(
                f"```json\n{json.dumps(response.json(), default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
        }
    )

    output_name = "job_id_png_to_mov"

    _id = response.json().get("_id", None)

    yield Output(
        output_name=output_name,
        value=_id,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(context.asset_key_for_output(output_name).path): MetadataValue.path(_id),
        }
    )


@multi_asset(
    outs={
        "png_to_mov": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            description="Todo",
        ),
    },
    ins={
        # "raw_to_oiio": AssetIn(
        #     AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "raw_to_oiio"]),
        # ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        # "image_sequence_raw": AssetIn(
        #     AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "image_sequence_raw"]),
        # ),
        "CONFIG_OIIO": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "CONFIG_OIIO"]),
        ),
        "job_id_raw": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "job_id_raw"]),
        ),
    }
)
def png_to_mov(
        context: AssetExecutionContext,
        # raw_to_oiio: List[Dict],
        render_version_directory: pathlib.Path,
        version: str,
        CONFIG_OIIO: ConfigOIIO,
        job_id_raw: str,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # https://stackoverflow.com/questions/24961127/how-to-create-a-video-from-images-with-ffmpeg
    # https://www.ffmpeg.media/articles/image-sequences-timelapse-photos-to-video

    # input_format_ = ".png"
    output_format_ = "mp4"
    input_dir: pathlib.Path = render_version_directory.joinpath(
        version,
        "oiio",
        f"oiio_png",
    )

    output_dir: pathlib.Path = render_version_directory.joinpath(
        version,
        "oiio",
        f"oiio_{output_format_}",
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # png_seq: List[pathlib.Path] = []

    # for d_image in raw_to_oiio:
    #     png: Union[pathlib.Path, None]
    #     png = d_image.get("png_out", None)
    #     if png is not None:
    #         png_seq.append(png)

    # context.log.debug(f"{png_seq = }")

    cmds: List[List[str]] = []
    ffmpeg_out = pathlib.Path(output_dir).joinpath(
        f"{output_format_}.{output_format_}"
    )

    # if bool(png_seq):
    # i_seq = []
    # f: pathlib.Path
    # for f in png_seq:
    #     i_seq.extend(["-i", f.as_posix()])

    # with tempfile.NamedTemporaryFile(
    #         delete=False,
    #         prefix="ffmpeg_images_list__",
    #         suffix=".txt",
    #         mode="w",
    # ) as file_out:
    #
    #     for f in png_seq:
    #         file_out.write(f"file {f.as_posix()}\n")

    # Todo:
    #  - [ ] add in timestamp
    #  - [ ] add out timestamp
    cmd: List[str] = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-framerate", f"{float(CONFIG_OIIO.fps):.3f}",
        # "-an",
        "-pattern_type", "glob",
        "-i", f"{input_dir.as_posix()}/*.png",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        ffmpeg_out.as_posix(),
    ]

    cmds.append(cmd)

    context.log.debug(f"{cmds = }")

    logs = submit_cmds(
        context=context,
        cmds=cmds,
    )

    # for image_ in image_sequence_raw:
    #     context.log.debug("Processing image %s", image_)
    #
    #     # Frame number based on image name (image.0123.png)
    #     f_no_ = re.findall(
    #         r"\.[0-9]+\.",
    #         image_.name
    #     )
    #
    #     if bool(f_no_):
    #         f_no = int(f_no_[-1].replace(".", ""))
    #     else:
    #         f_no = 0
    #
    #     context.log.debug(f"Frame number: {f_no}")
    #
    #     raw_buf, raw_spec = create_buf_from_raw(
    #         raw=image_
    #     )
    #
    #     processed_result = process_image(
    #         # raw_buf=raw_buf,
    #         raw_spec=raw_spec,
    #         context=context,
    #         image_filepath=image_,
    #         frame_number=f_no,
    #         kitsu_task_dict=get_kitsu_task_dict,
    #         CONFIG_OIIO=CONFIG_OIIO,
    #         version=version,
    #         output_dir=output_dir,
    #         create_exr_from_raw_with_custom_metadata=True,
    #         create_text_overlay=True,
    #         create_handle_overlay=True,
    #         create_png=True,
    #     )
    #     context.log.debug(f"{processed_result = }")
    #     results.append(processed_result)

    ##############
    # png_to_mov #
    ##############

    output_name = "png_to_mov"

    yield Output(
        output_name=output_name,
        value=ffmpeg_out,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.path(ffmpeg_out),
            "cmds": MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(cmds)}\n```"
            ),
            "logs": MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(logs)}\n```"
            ),
            # "ffmpeg_out": MetadataValue.path(ffmpeg_out),
        }
    )
