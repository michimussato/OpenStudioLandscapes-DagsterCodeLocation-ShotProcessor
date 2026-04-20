from dagster import (
    DefaultSensorStatus,
    AssetSelection,
    AutomationConditionSensorDefinition,
)

shot_processor_auto_materialize_sensor = AutomationConditionSensorDefinition(
    "ShotProcessor_AutoMaterializeSensor",
    target=AssetSelection.all(include_sources=False),
    minimum_interval_seconds=15,
    default_status=DefaultSensorStatus.RUNNING,
)
