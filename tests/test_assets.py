# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#unit-test-examples
import json
import pathlib
import shutil
from typing import Generator, List, Any, Dict

# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#upstream-dependencies

from dagster import build_asset_context, get_dagster_logger, Output, AssetMaterialization

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets import (
    image_sequence,
    raw_to_oiio,
)
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO

LOGGER = get_dagster_logger(__name__)


fixtures = pathlib.Path(__file__).parent / "fixtures"


def test_image_sequence() -> None:

    context = build_asset_context()

    expected = [
        fixtures / "v123/raw/sh030_001.1200.exr",
        fixtures / "v123/raw/sh030_001.1201.exr",
        fixtures / "v123/raw/sh030_001.1250.exr",
        fixtures / "v123/raw/sh030_001.1251.exr",
    ]

    from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
    config = DefaultConstants()

    asset_return_generator: Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None] = image_sequence(
        context=context,
        render_version_directory=fixtures,
        output_format="exr",
        version="v123",
        CONFIG=config,
    )

    result = [i for i in asset_return_generator]

    output: Output = result[0]
    actual = output.value
    asset_materialization: AssetMaterialization = result[1]

    assert actual == expected


def test_raw_to_oiio() -> None:

    context = build_asset_context()

    oiio_out = fixtures / "v123/oiio"

    expected = [

        {
            'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1200.exr',
            'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1200.exr',
            'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1200.exr',
        },
        {
            'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1201.exr',
            'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1201.exr',
            'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1201.exr',
        },
        {
            'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1250.exr',
            'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1250.exr',
            'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1250.exr',
        },
        {
            'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1251.exr',
            'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1251.exr',
            'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1251.exr',
        },
    ]

    with open(fixtures / "kitsu_task_dict.json") as fr:
        kitsu_task_dict = json.load(fr)

    from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
    config = DefaultConstants()

    config_oiio: ConfigOIIO = ConfigOIIO()

    image_sequence: List[pathlib.Path] = [
        fixtures / "v123/raw/sh030_001.1200.exr",
        fixtures / "v123/raw/sh030_001.1201.exr",
        fixtures / "v123/raw/sh030_001.1250.exr",
        fixtures / "v123/raw/sh030_001.1251.exr",
    ]

    # asset_return_generator: Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None] = list(raw_to_oiio(
    result = list(
        raw_to_oiio(
            context=context,
            get_kitsu_task_dict=kitsu_task_dict,
            version="v123",
            render_version_directory=fixtures,
            CONFIG=config,
            CONFIG_OIIO=config_oiio,
            image_sequence=image_sequence,
        )
    )

    shutil.rmtree(oiio_out)

    output: Output = result[0]
    actual = output.value
    asset_materialization: AssetMaterialization = result[1]

    assert actual == expected
