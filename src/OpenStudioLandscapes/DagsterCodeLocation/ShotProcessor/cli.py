import sys
import argparse
import pathlib
import textwrap
import logging

from dagster import (
    get_dagster_logger,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import run_shot_processor, ShotProcessorArgs

__author__ = "Michael Mussato"
__copyright__ = "Michael Mussato"
__url__ = "https://github.com/michimussato/OpenStudioLandscapes"
__license__ = "GNU Affero General Public License v3.0"

LOGGER = get_dagster_logger(__name__)


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args) -> ShotProcessorArgs:
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Takes an input directory where the raw EXR renders live, "
                                                 "and creates a new EXR sequence based on them with "
                                                 "additional metadata. It also creates handle and text overlay "
                                                 "EXR sequences for comp and editorial. The original raw EXR files "
                                                 "remain unchanged by this processor.")
    # parser.add_argument(
    #     "--version",
    #     action="version",
    #     version=f"moon-clock {__version__}",
    # )
    # parser.add_argument(dest="n", help="n-th Fibonacci number", type=int, metavar="INT")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    parser.add_argument(
        "--kitsu-task-json",
        dest="kitsu_task_json",
        help="The full path to the Kitsu task JSON file.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--exr-sequence-dir",
        dest="exr_sequence_dir",
        help="The full path to the directory containing the EXR sequence.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        help="The full path to the output base directory. Subdirectories will be created.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--fps",
        dest="fps",
        help="The frame rate of the sequence.",
        type=float,
        default=ShotProcessorArgs.fps,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--text-border",
        dest="text_border",
        help="The separation of the text from the frame.",
        type=int,
        default=ShotProcessorArgs.text_border,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--text-spacing",
        dest="text_spacing",
        help="The text line spacing.",
        type=int,
        default=ShotProcessorArgs.text_spacing,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--handle-marker-height",
        dest="handle_marker_height",
        help="The height of the handle marker.",
        type=int,
        default=ShotProcessorArgs.handle_marker_height,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--overlay-text-size-frame",
        dest="overlay_text_size_frame",
        help="The size of the frame number.",
        type=int,
        default=ShotProcessorArgs.overlay_text_size_frame,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    parser.add_argument(
        "--overlay-text-size-scaledown",
        dest="overlay_text_size_scaledown",
        help="The rest of the text will be scaled down by this amount.",
        type=int,
        default=ShotProcessorArgs.overlay_text_size_scaledown,
        required=False,
        # action="store_const",
        # const=logging.DEBUG,
    )
    return ShotProcessorArgs(**vars(parser.parse_args(args)))


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
    LOGGER.setLevel(loglevel)


def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)
    run_shot_processor(args)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(textwrap.dedent("""
            Wrong entry point.
            Run `shot-processor --help` for more information.
            """))
