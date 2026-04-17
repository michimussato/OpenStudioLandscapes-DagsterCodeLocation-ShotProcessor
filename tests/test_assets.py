import datetime
import json
import pathlib
import shutil
import subprocess
import textwrap
from typing import Generator, List, Any

import pytest
import yaml
# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#unit-test-examples

# https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#upstream-dependencies

from dagster import build_asset_context, Output, AssetMaterialization

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.assets import (
    image_sequence_raw,
    # raw_to_oiio,
)
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO


CLEANUP_ENABLED = False


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
def raw_in():
    # Use fixtures:
    # - [Pytest - How to use fixtures](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)
    # before test - create resource
    raw_in_ = fixtures / "v123" / "raw"
    yield raw_in_


@pytest.fixture
def oiio_out():
    # Use fixtures:
    # - [Pytest - How to use fixtures](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)
    # before test - create resource
    oiio_out_ = fixtures / "v123" / "oiio_out"
    yield oiio_out_
    # after test - remove resource
    if CLEANUP_ENABLED:
        shutil.rmtree(oiio_out_)


@pytest.fixture
def oiio_config():
    # Use fixtures:
    # - [Pytest - How to use fixtures](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)
    # before test - create resource
    oiio_config_ = fixtures / "oiio_config.yaml"

    with open(oiio_config_, "r") as fr:
        oiio_config_dict = yaml.safe_load(fr)

    oiio_config: ConfigOIIO = ConfigOIIO(
        **oiio_config_dict
    )

    yield oiio_config


@pytest.fixture
def kitsu_task_dict():
    # Use fixtures:
    # - [Pytest - How to use fixtures](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html)
    # before test - create resource
    kitsu_task_json = fixtures / "kitsu_task_dict.json"

    with open(kitsu_task_json, "r") as fr:
        kitsu_task_dict = json.load(fr)

    return kitsu_task_dict


@pytest.mark.skip("Not Implemented")
def test_get_overlay_text_buf() -> None:
    ...


@pytest.mark.skip("Not Implemented")
def test_get_overlay_handle_buf() -> None:
    ...


def test_get_frame_number(
    raw_in,
) -> None:
    from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import get_frame_number

    result = get_frame_number(
        image=raw_in / "sh030_001.1200.exr",
    )

    assert result == 1200


def test_create_handle_overlay(
    raw_in,
    oiio_out,
    oiio_config,
    kitsu_task_dict,
) -> None:
    from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import create_handle_overlay, get_frame_number

    raw_in_ = raw_in / "sh030_001.1200.exr"
    exr_out = oiio_out / "out" / "oiio_overlay_handle"

    result = create_handle_overlay(
        exr_src=raw_in_,
        CONFIG_OIIO=oiio_config,
        kitsu_task_dict=kitsu_task_dict,
        version="v123",
        frame_number=get_frame_number(raw_in_),
        output_dir=exr_out
    )

    assert result == exr_out / "sh030_001.1200.exr"
    assert result.exists()

    info = subprocess.check_output(
        [
            "oiiotool",
            "--info",
            "-v",
            result.as_posix()
        ]
    )

    info_expected = textwrap.dedent(
        f"""\
        Reading {result.as_posix()}
        {result.as_posix()} :  960 x  540, 4 channel, float openexr
            channel list: R, G, B, A
            Camera: "Camera"
            compression: "zip"
            cycles.ViewLayer.render_time: "00:13.99"
            cycles.ViewLayer.samples: "16"
            cycles.ViewLayer.synchronization_time: "00:00.01"
            cycles.ViewLayer.total_time: "00:14.01"
            Date: "2026/04/04 10:33:08"
            DateTime: "{datetime.datetime.now().strftime('%Y:%m:%d %H:%M:%S')}"
            File: "/data/share/AWSPortalRoot1/fixtures/blender/sh030_001.blend"
            Frame: "1200"
            openstudiolandscapes.data.resolution: "960x540"
            openstudiolandscapes.fps: "25.000"
            openstudiolandscapes.is_handle: 1
            openstudiolandscapes.kitsu.entity.data.fps: "25.000"
            openstudiolandscapes.kitsu.entity.data.frame_in: 1201
            openstudiolandscapes.kitsu.entity.data.frame_out: 1250
            openstudiolandscapes.kitsu.entity.data.resolution: "960x540"
            openstudiolandscapes.kitsu.entity.name: "SH030"
            openstudiolandscapes.kitsu.project.name: "Test Production"
            openstudiolandscapes.kitsu.sequence.name: "SQ010"
            openstudiolandscapes.kitsu.task.id: "b0cfdac7-afa9-4382-a75d-3c80a388e136"
            openstudiolandscapes.version: "v123"
            PixelAspectRatio: 1
            RenderTime: "00:14.01"
            Scene: "Scene"
            screenWindowCenter: 0, 0
            screenWindowWidth: 1
            Time: "00:00:48:00"
            oiio:subimages: 1
            openexr:lineOrder: "increasingY"
        """
    )

    assert info.decode('utf-8') == info_expected


def test_exr_with_custom_metadata(
    raw_in,
    oiio_out,
    oiio_config,
    kitsu_task_dict,
) -> None:
    from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import exr_with_custom_metadata, get_frame_number

    raw_in_ = raw_in / "sh030_001.1200.exr"
    exr_out = oiio_out / "out" / "oiio_exr"

    result = exr_with_custom_metadata(
        exr_src=raw_in_,
        CONFIG_OIIO=oiio_config,
        kitsu_task_dict=kitsu_task_dict,
        version="v123",
        frame_number=get_frame_number(raw_in_),
        output_dir=exr_out
    )

    assert result == exr_out / "sh030_001.1200.exr"
    assert result.exists()

    info = subprocess.check_output(
        [
            "oiiotool",
            "--info",
            "-v",
            result.as_posix()
        ]
    )

    info_expected = textwrap.dedent(
        f"""\
        Reading {result.as_posix()}
        {result.as_posix()} :  960 x  540, 3 channel, float openexr
            channel list: R, G, B
            Camera: "Camera"
            compression: "zip"
            cycles.ViewLayer.render_time: "00:13.99"
            cycles.ViewLayer.samples: "16"
            cycles.ViewLayer.synchronization_time: "00:00.01"
            cycles.ViewLayer.total_time: "00:14.01"
            Date: "2026/04/04 10:33:08"
            DateTime: "{datetime.datetime.now().strftime('%Y:%m:%d %H:%M:%S')}"
            File: "/data/share/AWSPortalRoot1/fixtures/blender/sh030_001.blend"
            Frame: "1200"
            openstudiolandscapes.data.resolution: "960x540"
            openstudiolandscapes.fps: "25.000"
            openstudiolandscapes.is_handle: 1
            openstudiolandscapes.kitsu.entity.data.fps: "25.000"
            openstudiolandscapes.kitsu.entity.data.frame_in: 1201
            openstudiolandscapes.kitsu.entity.data.frame_out: 1250
            openstudiolandscapes.kitsu.entity.data.resolution: "960x540"
            openstudiolandscapes.kitsu.entity.name: "SH030"
            openstudiolandscapes.kitsu.project.name: "Test Production"
            openstudiolandscapes.kitsu.sequence.name: "SQ010"
            openstudiolandscapes.kitsu.task.id: "b0cfdac7-afa9-4382-a75d-3c80a388e136"
            openstudiolandscapes.version: "v123"
            PixelAspectRatio: 1
            RenderTime: "00:14.01"
            Scene: "Scene"
            screenWindowCenter: 0, 0
            screenWindowWidth: 1
            Time: "00:00:48:00"
            oiio:subimages: 1
            openexr:lineOrder: "increasingY"
        """
    )

    assert info.decode('utf-8') == info_expected


def test_create_png(
    raw_in,
    oiio_out,
    oiio_config,
    kitsu_task_dict,
) -> None:
    from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import create_png, get_frame_number

    raw_in_ = raw_in / "sh030_001.1200.exr"
    png_out = oiio_out / "out" / "oiio_proxy_png"

    result = create_png(
        exr_src=raw_in_,
        CONFIG_OIIO=oiio_config,
        kitsu_task_dict=kitsu_task_dict,
        version="v123",
        frame_number=get_frame_number(raw_in_),
        output_dir=png_out
    )

    assert result == png_out / "sh030_001.1200.png"
    assert result.exists()


# # @pytest.mark.usefixtures("fixture_oiio_out")
# def test_raw_to_oiio(
#     oiio_out,
# ) -> None:
#
#     context = build_asset_context()
#
#     # oiio_out = fixture_oiio_out
#
#     expected = [
#         {
#             'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1200.exr',
#             'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1200.exr',
#             'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1200.exr',
#         },
#         {
#             'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1201.exr',
#             'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1201.exr',
#             'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1201.exr',
#         },
#         {
#             'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1250.exr',
#             'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1250.exr',
#             'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1250.exr',
#         },
#         {
#             'exr_touched_out': oiio_out / 'oiio_exr/sh030_001.1251.exr',
#             'overlay_handle_buf_out': oiio_out / 'oiio_overlay_handle/sh030_001.1251.exr',
#             'overlay_text_buf_out': oiio_out / 'oiio_overlay_text/sh030_001.1251.exr',
#         },
#     ]
#
#     with open(fixtures / "kitsu_task_dict.json") as fr:
#         kitsu_task_dict = json.load(fr)
#
#     config_oiio: ConfigOIIO = ConfigOIIO()
#
#     image_sequence: List[pathlib.Path] = [
#         fixtures / "v123/raw/sh030_001.1200.exr",
#         fixtures / "v123/raw/sh030_001.1201.exr",
#         fixtures / "v123/raw/sh030_001.1250.exr",
#         fixtures / "v123/raw/sh030_001.1251.exr",
#     ]
#
#     # asset_return_generator: Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None] = list(raw_to_oiio(
#     # Todo:
#     #  - [ ] change to multi_asset test if all this works
#     result = list(
#         raw_to_oiio(
#             context=context,
#             get_kitsu_task_dict=kitsu_task_dict,
#             version="v123",
#             render_version_directory=fixtures,
#             CONFIG_OIIO=config_oiio,
#             image_sequence_raw=image_sequence,
#         )
#     )
#
#     output: Output = result[0]
#     actual = output.value
#     asset_materialization: AssetMaterialization = result[1]
#
#     assert actual == expected
