import json
import pathlib
import shutil
from typing import Generator, List, Any

import pytest
# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#unit-test-examples

# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#upstream-dependencies

from dagster import build_asset_context, Output, AssetMaterialization

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets import (
    image_sequence_raw,
    raw_to_oiio,
)
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO


CLEANUP_ENABLED = True


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

    asset_return_generator: Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None] = image_sequence_raw(
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


@pytest.fixture
def oiio_out():
    # Use fixtures:
    # - [Pytest - How to use fixtures](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)
    # before test - create resource
    oiio_out = fixtures / "v123/oiio"
    yield oiio_out
    # after test - remove resource
    if CLEANUP_ENABLED:
        shutil.rmtree(oiio_out)

# @pytest.mark.usefixtures("fixture_oiio_out")
def test_raw_to_oiio(
    oiio_out,
) -> None:

    context = build_asset_context()

    # oiio_out = fixture_oiio_out

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

    config_oiio: ConfigOIIO = ConfigOIIO()

    image_sequence: List[pathlib.Path] = [
        fixtures / "v123/raw/sh030_001.1200.exr",
        fixtures / "v123/raw/sh030_001.1201.exr",
        fixtures / "v123/raw/sh030_001.1250.exr",
        fixtures / "v123/raw/sh030_001.1251.exr",
    ]

    # asset_return_generator: Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None] = list(raw_to_oiio(
    # Todo:
    #  - [ ] change to multi_asset test if all this works
    result = list(
        raw_to_oiio(
            context=context,
            get_kitsu_task_dict=kitsu_task_dict,
            version="v123",
            render_version_directory=fixtures,
            CONFIG_OIIO=config_oiio,
            image_sequence_raw=image_sequence,
        )
    )

    output: Output = result[0]
    actual = output.value
    asset_materialization: AssetMaterialization = result[1]

    assert actual == expected
