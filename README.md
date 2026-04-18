[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

---

<!-- TOC -->
* [OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor](#openstudiolandscapes-dagstercodelocation-shotprocessor)
  * [Brief](#brief)
  * [Usage](#usage)
    * [API](#api)
    * [CLI](#cli)
* [Development](#development)
  * [Dagster dev](#dagster-dev)
  * [pytest](#pytest)
<!-- TOC -->

---

# OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor

- [README `png_to_mov`](src/OpenStudioLandscapes/DagsterCodeLocation/ShotProcessor/png_to_mov/README.md)

Status: `WIP`

> [!NOTE]
> 
> This package was scaffolded with `dagster==1.9.11`
> 
> ```shell
> dagster project scaffold --name OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor
> cd OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor
> git init --initial-branch main
> git remote add origin https://github.com/michimussato/OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor.git
> git add *
> git commit -a -m "initial commit"
> git push -u origin main
> ```

## Brief

A package to run post-render jobs on the resulting `EXR` files.

## Usage

### API

- [ ] Todo: Update

```python
from typing import Any, Generator, List, Union

from dagster import (
    OpExecutionContext,
    AssetExecutionContext
)

from OpenStudioLandscapes.DagsterCodeLocation import ShotProcessor

...
```

### CLI

The CLI takes **one input file** and creates **one output file**.
No sequences are supported. This CLI is meant to run 
on a farm where workers can batch process input files individually
in parallel.

```
$ shot-processor --help
usage: shot-processor [-h] [-v] [-vv] --kitsu-task-json KITSU_TASK_JSON --oiio-config-yaml OIIO_CONFIG_YAML --version VERSION --frame-number FRAME_NUMBER --exr-image EXR_IMAGE --output-dir OUTPUT_DIR {create-text-overlay,create-handle-overlay,exr-with-custom-metadata,create-png} ...

Takes an input EXR and creates a new file based on it by specifying the relevant sub-command.

positional arguments:
  {create-text-overlay,create-handle-overlay,exr-with-custom-metadata,create-png}

options:
  -h, --help            show this help message and exit
  -v, --verbose         set loglevel to INFO
  -vv, --very-verbose   set loglevel to DEBUG
  --kitsu-task-json KITSU_TASK_JSON
                        The full path to the Kitsu task JSON file.
  --oiio-config-yaml OIIO_CONFIG_YAML
                        The full path to the OIIO config YAML file.
  --version VERSION     The version (iteration) number.
  --frame-number FRAME_NUMBER
                        The frame number.
  --exr-image EXR_IMAGE
                        The full path to the EXR file.
  --output-dir OUTPUT_DIR
                        The full path to the output base directory. Subdirectories will be created
```

> [!TIP]
> 
> Reinstall this package inside the container it was deployed to
> (i.e. OpenStudioLandscapes-Dagster container):
> 
> ```shell
> docker exec -it <container> bash
> pip3 install \
>     --root-user-action=ignore \
>     --force-reinstall \
>     --editable 'OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor @ git+https://github.com/michimussato/OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor.git@main'
> ```

# Development

## Dagster dev

```shell
pip install --force-reinstall --editable .[dev]

dagster dev --workspace workspace.yaml
```

## pytest

```shell
pip install --editable .[dev]
pytest -s -vv ./tests
```

Add `-s` (equivalent to `--capture=no`) to `pytest` runner
in case `print` or `LOGGER` output is required.

> [!TIP]
> 
> In Pycharm:
> ![Modify Run Configuration](media/images/Screenshot_20260407_110349.png)



```
dagster._core.errors.DagsterInvalidDefinitionError: Input asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_Reader", "read_job_yaml"]' for asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor_create_text_overlay", "job"]' is not produced by any of the provided asset ops and is not one of the provided sources.

  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 417, in __init__
    self._loaded_repositories: Optional[LoadedRepositories] = LoadedRepositories(
                                                              ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 271, in __init__
    repo_def = recon_repo.get_definition()
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 111, in get_definition
    return reconstruct_repository_def_from_pointer(self.pointer, self.repository_load_data)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 798, in reconstruct_repository_def_from_pointer
    repo_def = _repository_def_from_target_def_inner(
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 708, in _repository_def_from_target_def_inner
    return target.get_repository_def()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_utils/cached_method.py", line 135, in _cached_method_wrapper
    result = method(self, *args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 572, in get_repository_def
    return _create_repository_using_definitions_args(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 278, in _create_repository_using_definitions_args
    @repository(
     ^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/decorators/repository_decorator.py", line 146, in __call__
    repository_data = CachingRepositoryData.from_list(
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data.py", line 380, in from_list
    return build_caching_repository_data_from_list(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data_builder.py", line 309, in build_caching_repository_data_from_list
    asset_graph = AssetGraph.from_assets(
                  ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 250, in from_assets
    assets_defs = cls.normalize_assets(assets)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 206, in normalize_assets
    resolved_deps = ResolvedAssetDependencies(assets_defs, [])
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 24, in __init__
    self._deps_by_assets_def_id = resolve_assets_def_deps(assets_defs, source_assets)
                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 207, in resolve_assets_def_deps
    raise DagsterInvalidDefinitionError(msg)
```

```
dagster._core.errors.DagsterInvalidDefinitionError: Input asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_PreProcessor", "render_output_directory"]' for asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor_create_text_overlay", "plugin_info_model"]' is not produced by any of the provided asset ops and is not one of the provided sources.

  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 417, in __init__
    self._loaded_repositories: Optional[LoadedRepositories] = LoadedRepositories(
                                                              ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 271, in __init__
    repo_def = recon_repo.get_definition()
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 111, in get_definition
    return reconstruct_repository_def_from_pointer(self.pointer, self.repository_load_data)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 798, in reconstruct_repository_def_from_pointer
    repo_def = _repository_def_from_target_def_inner(
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 708, in _repository_def_from_target_def_inner
    return target.get_repository_def()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_utils/cached_method.py", line 135, in _cached_method_wrapper
    result = method(self, *args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 572, in get_repository_def
    return _create_repository_using_definitions_args(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 278, in _create_repository_using_definitions_args
    @repository(
     ^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/decorators/repository_decorator.py", line 146, in __call__
    repository_data = CachingRepositoryData.from_list(
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data.py", line 380, in from_list
    return build_caching_repository_data_from_list(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data_builder.py", line 309, in build_caching_repository_data_from_list
    asset_graph = AssetGraph.from_assets(
                  ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 250, in from_assets
    assets_defs = cls.normalize_assets(assets)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 206, in normalize_assets
    resolved_deps = ResolvedAssetDependencies(assets_defs, [])
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 24, in __init__
    self._deps_by_assets_def_id = resolve_assets_def_deps(assets_defs, source_assets)
                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 207, in resolve_assets_def_deps
    raise DagsterInvalidDefinitionError(msg)
```

```
dagster._core.errors.DagsterInvalidDefinitionError: Input asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_Reader", "read_job_yaml"]' for asset '["OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor_create_text_overlay", "job"]' is not produced by any of the provided asset ops and is not one of the provided sources.

  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 417, in __init__
    self._loaded_repositories: Optional[LoadedRepositories] = LoadedRepositories(
                                                              ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_grpc/server.py", line 271, in __init__
    repo_def = recon_repo.get_definition()
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 111, in get_definition
    return reconstruct_repository_def_from_pointer(self.pointer, self.repository_load_data)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 798, in reconstruct_repository_def_from_pointer
    repo_def = _repository_def_from_target_def_inner(
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/reconstruct.py", line 708, in _repository_def_from_target_def_inner
    return target.get_repository_def()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_utils/cached_method.py", line 135, in _cached_method_wrapper
    result = method(self, *args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 572, in get_repository_def
    return _create_repository_using_definitions_args(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/definitions_class.py", line 278, in _create_repository_using_definitions_args
    @repository(
     ^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/decorators/repository_decorator.py", line 146, in __call__
    repository_data = CachingRepositoryData.from_list(
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data.py", line 380, in from_list
    return build_caching_repository_data_from_list(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/repository_definition/repository_data_builder.py", line 309, in build_caching_repository_data_from_list
    asset_graph = AssetGraph.from_assets(
                  ^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 250, in from_assets
    assets_defs = cls.normalize_assets(assets)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/asset_graph.py", line 206, in normalize_assets
    resolved_deps = ResolvedAssetDependencies(assets_defs, [])
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 24, in __init__
    self._deps_by_assets_def_id = resolve_assets_def_deps(assets_defs, source_assets)
                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/dagster/_core/definitions/resolved_asset_deps.py", line 207, in resolve_assets_def_deps
    raise DagsterInvalidDefinitionError(msg)
```