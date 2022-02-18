import os
import pkgutil
import re
from importlib import import_module
from types import ModuleType
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Union,
    cast,
)

from dagster import check
from dagster.utils import merge_dicts

from ..definitions.executor_definition import ExecutorDefinition
from ..definitions.job_definition import JobDefinition
from ..definitions.op_definition import OpDefinition
from ..definitions.resource_definition import ResourceDefinition
from ..errors import DagsterInvalidDefinitionError
from .asset import AssetsDefinition
from .assets_job import build_assets_job, build_root_manager, build_source_assets_by_key
from .source_asset import SourceAsset


class AssetGroup(
    NamedTuple(
        "_AssetGroup",
        [
            ("assets", Sequence[AssetsDefinition]),
            ("source_assets", Sequence[SourceAsset]),
            ("resource_defs", Mapping[str, ResourceDefinition]),
            ("executor_def", Optional[ExecutorDefinition]),
        ],
    )
):
    def __new__(
        cls,
        assets: Sequence[AssetsDefinition],
        source_assets: Optional[Sequence[SourceAsset]] = None,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        executor_def: Optional[ExecutorDefinition] = None,
    ):
        from dagster.core.definitions.graph_definition import default_job_io_manager

        check.list_param(assets, "assets", of_type=AssetsDefinition)
        source_assets = check.opt_list_param(source_assets, "source_assets", of_type=SourceAsset)
        resource_defs = check.opt_dict_param(
            resource_defs, "resource_defs", key_type=str, value_type=ResourceDefinition
        )
        executor_def = check.opt_inst_param(executor_def, "executor_def", ExecutorDefinition)

        source_assets_by_key = build_source_assets_by_key(source_assets)
        root_manager = build_root_manager(source_assets_by_key)

        if "root_manager" in resource_defs:
            raise DagsterInvalidDefinitionError(
                "Resource dictionary included resource with key 'root_manager', "
                "which is a reserved resource keyword in Dagster. Please change "
                "this key, and then change all places that require this key to "
                "a new value."
            )
        # In the case of collisions, merge_dicts takes values from the dictionary latest in the list, so we place the user provided resource defs after the defaults.
        resource_defs = merge_dicts(
            {"root_manager": root_manager, "io_manager": default_job_io_manager},
            resource_defs,
        )

        _validate_resource_reqs_for_asset_group(
            asset_list=assets, source_assets=source_assets, resource_defs=resource_defs
        )

        return super(AssetGroup, cls).__new__(
            cls,
            assets=assets,
            source_assets=source_assets,
            resource_defs=resource_defs,
            executor_def=executor_def,
        )

    @property
    def all_assets_job_name(self) -> str:
        """The name of the mega-job that the provided list of assets is coerced into."""
        return "__ASSET_GROUP"

    def build_job(
        self,
        name: str,
        selection: Optional[Union[str, List[str]]] = None,
        executor_def: Optional[ExecutorDefinition] = None,
        tags: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
    ) -> JobDefinition:
        from dagster.core.selector.subset_selector import parse_asset_selection

        check.str_param(name, "name")

        if not isinstance(selection, str):
            selection = check.opt_list_param(selection, "selection", of_type=str)
        else:
            selection = [selection]
        executor_def = check.opt_inst_param(executor_def, "executor_def", ExecutorDefinition)
        description = check.opt_str_param(description, "description")

        if selection:
            selected_asset_keys = parse_asset_selection(self.assets, selection)

            included_assets, excluded_assets = self._selected_asset_defs(selected_asset_keys)
        else:
            selected_asset_keys = set()
            for asset in self.assets:
                selected_asset_keys.update(asset.asset_keys)
            included_assets = cast(List[AssetsDefinition], self.assets)
            # Call to list(...) serves as a copy constructor, so that we don't
            # accidentally add to the original list
            excluded_assets = list(self.source_assets)

        return build_assets_job(
            name=name,
            assets=included_assets,
            source_assets=excluded_assets,
            resource_defs=self.resource_defs,
            executor_def=self.executor_def,
            description=description,
            tags=tags,
            config=self._config_for_selected_asset_keys(selected_asset_keys, included_assets),
        )

    def _config_for_selected_asset_keys(self, job_selected_asset_keys, included_assets):
        from dagster import ConfigMapping, Field, AssetKey

        job_selected_asset_keys = {".".join(ak.path) for ak in job_selected_asset_keys}

        def _asset_selection(config):
            config_selected_asset_keys = config.get("selected_assets")
            op_config = {}
            for asset in included_assets:
                if not asset.can_subset:
                    continue
                asset_keys_for_op = {".".join(ak.path) for ak in asset.asset_keys}
                op_config[asset.op.name] = {
                    "config": {
                        "selected_assets": list(
                            asset_keys_for_op.intersection(config_selected_asset_keys)
                        )
                        if config_selected_asset_keys
                        else list(asset_keys_for_op)
                    }
                }
            return {"ops": op_config}

        return ConfigMapping(
            config_fn=_asset_selection,
            config_schema={"selected_assets": Field(list, is_required=False)},
        )

    def _selected_asset_defs(self, selected_asset_keys):
        included_assets = set()
        excluded_assets = set()
        for asset in self.assets:
            selected_subset = selected_asset_keys.intersection(asset.asset_keys)
            # all assets selected
            if selected_subset == asset.asset_keys:
                included_assets.add(asset)
            # no assets selected
            elif selected_subset == set():
                excluded_assets.add(asset)
            # subset selected
            else:
                if asset.can_subset:
                    included_assets.add(asset.subset(selected_subset))
                    # excluded_assets.add(asset.subset(asset.asset_keys - selected_subset))
                else:
                    # TODO: warn
                    included_assets.add(asset)
        return list(included_assets), list(excluded_assets)

    @staticmethod
    def from_package_module(
        package_module: ModuleType,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        executor_def: Optional[ExecutorDefinition] = None,
    ) -> "AssetGroup":
        """
        Constructs an AssetGroup that includes all asset definitions in all sub-modules of the
        given package module.

        A package module is the result of importing a package.

        Args:
            package_module (ModuleType): The package module to looks for assets inside.
            resource_defs (Optional[Mapping[str, ResourceDefinition]]): A dictionary of resource
                definitions to include on the returned asset group.
            executor_def (Optional[ExecutorDefinition]): An executor to include on the returned
                asset group.

        Returns:
            AssetGroup: An asset group with all the assets in the package.
        """
        assets = set()
        source_assets = set()
        for module in _find_modules_in_package(package_module):
            for attr in dir(module):
                value = getattr(module, attr)
                if isinstance(value, AssetsDefinition):
                    assets.add(value)
                if isinstance(value, SourceAsset):
                    source_assets.add(value)

        return AssetGroup(
            assets=list(assets),
            source_assets=list(source_assets),
            resource_defs=resource_defs,
            executor_def=executor_def,
        )

    @staticmethod
    def from_package_name(
        package_name: str,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        executor_def: Optional[ExecutorDefinition] = None,
    ) -> "AssetGroup":
        """
        Constructs an AssetGroup that includes all asset definitions in all sub-modules of the
        given package.

        Args:
            package_name (str): The name of a Python package to look for assets inside.
            resource_defs (Optional[Mapping[str, ResourceDefinition]]): A dictionary of resource
                definitions to include on the returned asset group.
            executor_def (Optional[ExecutorDefinition]): An executor to include on the returned
                asset group.

        Returns:
            AssetGroup: An asset group with all the assets in the package.
        """
        package_module = import_module(package_name)
        return AssetGroup.from_package_module(
            package_module, resource_defs=resource_defs, executor_def=executor_def
        )


def _find_modules_in_package(package_module: ModuleType) -> Iterable[ModuleType]:
    yield package_module
    package_path = package_module.__file__
    if package_path:
        for _, modname, is_pkg in pkgutil.walk_packages([os.path.dirname(package_path)]):
            submodule = import_module(f"{package_module.__name__}.{modname}")
            if is_pkg:
                yield from _find_modules_in_package(submodule)
            else:
                yield submodule
    else:
        raise ValueError(
            f"Tried find modules in package {package_module}, but its __file__ is None"
        )


def _validate_resource_reqs_for_asset_group(
    asset_list: Sequence[AssetsDefinition],
    source_assets: Sequence[SourceAsset],
    resource_defs: Mapping[str, ResourceDefinition],
):
    present_resource_keys = set(resource_defs.keys())
    for asset_def in asset_list:
        resource_keys = set(asset_def.op.required_resource_keys or {})
        missing_resource_keys = list(set(resource_keys) - present_resource_keys)
        if missing_resource_keys:
            raise DagsterInvalidDefinitionError(
                f"AssetGroup is missing required resource keys for asset '{asset_def.op.name}'. Missing resource keys: {missing_resource_keys}"
            )

        for asset_key, output_def in asset_def.output_defs_by_asset_key.items():
            if output_def.io_manager_key and output_def.io_manager_key not in present_resource_keys:
                raise DagsterInvalidDefinitionError(
                    f"Output '{output_def.name}' with AssetKey '{asset_key}' requires io manager '{output_def.io_manager_key}' but was not provided on asset group. Provided resources: {sorted(list(present_resource_keys))}"
                )

    for source_asset in source_assets:
        if source_asset.io_manager_key and source_asset.io_manager_key not in present_resource_keys:
            raise DagsterInvalidDefinitionError(
                f"SourceAsset with key {source_asset.key} requires io manager with key '{source_asset.io_manager_key}', which was not provided on AssetGroup. Provided keys: {sorted(list(present_resource_keys))}"
            )

    for resource_key, resource_def in resource_defs.items():
        resource_keys = set(resource_def.required_resource_keys)
        missing_resource_keys = sorted(list(set(resource_keys) - present_resource_keys))
        if missing_resource_keys:
            raise DagsterInvalidDefinitionError(
                f"AssetGroup is missing required resource keys for resource '{resource_key}'. Missing resource keys: {missing_resource_keys}"
            )
