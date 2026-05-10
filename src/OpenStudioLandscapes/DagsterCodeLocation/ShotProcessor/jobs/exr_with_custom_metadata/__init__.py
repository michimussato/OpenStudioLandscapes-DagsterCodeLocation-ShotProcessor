JOB = "exr_with_custom_metadata"


GROUP = f"OpenStudioLandscapes_DagsterCodeLocation_ShotProcessor_OIIO_Processor_{JOB}"
# KEY_CONSTANTS_DEFAULT = [GROUP_CONSTANTS_DEFAULT, "Constants"]
KEY_PREFIX = [GROUP]

ASSET_HEADER = {
    "group_name": GROUP,
    "key_prefix": KEY_PREFIX,
}
