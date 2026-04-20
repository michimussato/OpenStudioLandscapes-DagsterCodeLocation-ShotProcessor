import json
import os
import pathlib

from typing import (
    Generator,
    Any,
    List,
)

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
)

# Asset data across code locations:
# - [SourceAsset](https://stackoverflow.com/q/79780791)
# - [AssetSpec](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/external-assets)
# - [Asset obervations](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/asset-observations)


GROUP_OIIO_PROCESSOR = "OpenStudioLandscapes_DagsterCodeLocation_ShotProcessor_OIIO_Processor"
KEY_OIIO_PROCESSOR = [GROUP_OIIO_PROCESSOR]

ASSET_HEADER_OIIO_PROCESSOR = {
    "group_name": GROUP_OIIO_PROCESSOR,
    "key_prefix": KEY_OIIO_PROCESSOR,
}


@multi_asset(
    ins={
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        ),
        # "render_output_directory": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        # ),
    },
    outs={
        "CONFIG_OIIO": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            dagster_type=ConfigOIIO,
            description="Todo",
        ),
        "CONFIG_OIIO_YAML": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            dagster_type=pathlib.Path,
            description="Todo",
        ),
    },
)
def CONFIG_OIIO(
        context: AssetExecutionContext,
        render_output_directory: pathlib.Path,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    config_oiio_yaml = render_output_directory.joinpath("config_oiio.yaml")
    config_oiio_yaml.parent.mkdir(parents=True, exist_ok=True)

    if config_oiio_yaml.exists():
        with open(config_oiio_yaml, "r") as fr:
            config_oiio_dict = yaml.safe_load(fr)

        config_oiio: ConfigOIIO = ConfigOIIO(
            **config_oiio_dict,
        )
    else:
        config_oiio: ConfigOIIO = ConfigOIIO()

        with open(config_oiio_yaml, "w") as fw:
            fw.write(yaml.safe_dump(config_oiio.model_dump_json(fallback=str, indent=2)))
        # config_oiio_yaml = {}

    # _out = render_version_directory / version
    # _out.mkdir(parents=True, exist_ok=True)

    # if bool(job_model.kitsu_task):
    #     entity_type = get_entity_type(get_kitsu_task_dict)
    #     if entity_type == 'Shot':
    #         # filename = f'{str(handles)}_{str(job_model.cut_in - job_model.handles).zfill(CONFIG.PADDING)}-{str(job_model.cut_out + job_model.handles).zfill(CONFIG.PADDING)}_{str(handles)}'
    #         # with open(_out / filename, "w") as fw:
    #         #     fw.write(f"{str(job_model.kitsu_task) = }")
    #         # with open(_out / "kitsu_task_id.txt", "w") as fw:
    #         #     fw.write(str(job_model.kitsu_task))
    #         with open(_out / "kitsu_task.json", "w") as fw:
    #             json.dump(
    #                 get_kitsu_task_dict,
    #                 fw,
    #                 indent=2,
    #                 default=str,
    #                 ensure_ascii=True,
    #                 sort_keys=True,
    #             )
    #
    # with open(pathlib.Path(__file__).parent / "config_oiio.yaml") as fw:
    #     fw.

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

    ####################
    # CONFIG_OIIO_YAML #
    ####################

    output_name = "CONFIG_OIIO_YAML"

    yield Output(
        output_name=output_name,
        value=config_oiio_yaml,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.path(config_oiio_yaml),
        },
    )


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    ins={
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        ),
        "output_format": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
    }
)
def image_sequence_raw(
        context: AssetExecutionContext,
        render_output_directory: pathlib.Path,
        output_format: str,
        CONFIG: DefaultConstants,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    sequence_dir: pathlib.Path = render_output_directory / CONFIG.RENDER_RAW_OUT

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
