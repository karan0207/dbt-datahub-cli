"""Parser for dbt manifest.json and catalog.json files."""

import json
import logging
from pathlib import Path
from typing import Any, Optional

from dbt_datahub_governance.exceptions import DbtParserError
from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel

logger = logging.getLogger(__name__)


class DbtManifestParser:
    """Parser for dbt manifest.json files."""

    SUPPORTED_RESOURCE_TYPES = {"model", "source", "seed", "snapshot"}

    def __init__(self, manifest_path: str | Path) -> None:
        self.manifest_path = Path(manifest_path)
        if not self.manifest_path.exists():
            raise DbtParserError(f"Manifest file not found: {self.manifest_path}")
        self._raw_manifest: Optional[dict[str, Any]] = None
        self._manifest: Optional[DbtManifest] = None

    def _load_raw_manifest(self) -> dict[str, Any]:
        if self._raw_manifest is None:
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    self._raw_manifest = json.load(f)
            except json.JSONDecodeError as e:
                raise DbtParserError(f"Invalid JSON in manifest file: {e}")
            except IOError as e:
                raise DbtParserError(f"Error reading manifest file: {e}")
        return self._raw_manifest

    def parse(self) -> DbtManifest:
        """Parse the manifest file and return a DbtManifest object."""
        if self._manifest is not None:
            return self._manifest

        raw = self._load_raw_manifest()
        metadata = raw.get("metadata", {})
        dbt_version = metadata.get("dbt_version", "unknown")

        models: dict[str, DbtModel] = {}
        for node_id, node_data in raw.get("nodes", {}).items():
            if node_data.get("resource_type", "") == "model":
                try:
                    model = DbtModel.from_manifest_node(node_data)
                    models[node_id] = model
                    logger.debug(f"Parsed model: {model.name}")
                except Exception as e:
                    logger.warning(f"Failed to parse node {node_id}: {e}")

        sources: dict[str, dict[str, Any]] = {
            source_id: source_data
            for source_id, source_data in raw.get("sources", {}).items()
        }

        self._manifest = DbtManifest(
            models=models,
            sources=sources,
            metadata=metadata,
            dbt_version=dbt_version,
        )
        logger.info(f"Parsed manifest with {len(models)} models and {len(sources)} sources")
        return self._manifest

    def get_models(self) -> dict[str, DbtModel]:
        """Get all parsed models."""
        return self.parse().models

    def get_model_dependencies(self, model: DbtModel) -> list[str]:
        """Get model dependencies."""
        return model.depends_on

    def get_model_upstream_models(self, model: DbtModel) -> list[DbtModel]:
        """Get upstream models."""
        return self.parse().get_upstream_models(model)


class DbtCatalogParser:
    """Parser for dbt catalog.json files."""

    def __init__(self, catalog_path: str | Path) -> None:
        self.catalog_path = Path(catalog_path)
        self._raw_catalog: Optional[dict[str, Any]] = None

    def exists(self) -> bool:
        """Check if catalog file exists."""
        return self.catalog_path.exists()

    def _load_raw_catalog(self) -> dict[str, Any]:
        """Load raw catalog JSON."""
        if self._raw_catalog is None:
            if not self.exists():
                raise DbtParserError(f"Catalog file not found: {self.catalog_path}")
            try:
                with open(self.catalog_path, "r", encoding="utf-8") as f:
                    self._raw_catalog = json.load(f)
            except json.JSONDecodeError as e:
                raise DbtParserError(f"Invalid JSON in catalog file: {e}")
        return self._raw_catalog

    def get_column_info(self, model_unique_id: str) -> dict[str, dict[str, Any]]:
        """Get column information for a model from the catalog."""
        if not self.exists():
            return {}

        raw = self._load_raw_catalog()
        nodes = raw.get("nodes", {})

        if model_unique_id in nodes:
            node = nodes[model_unique_id]
            return {
                name: {
                    "name": info.get("name", name),
                    "type": info.get("type"),
                    "comment": info.get("comment"),
                    "index": info.get("index"),
                }
                for name, info in node.get("columns", {}).items()
            }
        return {}


def load_dbt_project(
    manifest_path: str | Path,
    catalog_path: Optional[str | Path] = None,
) -> DbtManifest:
    """Load a dbt project from manifest and optional catalog files."""
    parser = DbtManifestParser(manifest_path)
    manifest = parser.parse()

    if catalog_path:
        catalog_parser = DbtCatalogParser(catalog_path)
        if catalog_parser.exists():
            for model_id, model in manifest.models.items():
                column_info = catalog_parser.get_column_info(model_id)
                for col_name, col_data in column_info.items():
                    if col_name not in model.columns:
                        model.columns[col_name] = col_data
                    else:
                        model.columns[col_name].update(
                            {k: v for k, v in col_data.items() if v is not None}
                        )

    return manifest
