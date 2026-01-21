"""Data models for dbt artifacts."""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class DbtDependency:
    """Represents a dbt model dependency."""

    unique_id: str
    name: str
    resource_type: str
    database: Optional[str] = None
    schema: Optional[str] = None

    @classmethod
    def from_node(cls, node: dict[str, Any]) -> "DbtDependency":
        return cls(
            unique_id=node.get("unique_id", ""),
            name=node.get("name", ""),
            resource_type=node.get("resource_type", ""),
            database=node.get("database"),
            schema=node.get("schema"),
        )


@dataclass
class DbtModel:
    """Represents a dbt model from manifest.json."""

    unique_id: str
    name: str
    database: Optional[str]
    schema: str
    description: Optional[str]
    resource_type: str
    package_name: str
    path: str
    original_file_path: str
    depends_on: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        if self.database:
            return f"{self.database}.{self.schema}.{self.name}"
        return f"{self.schema}.{self.name}"

    @classmethod
    def from_manifest_node(cls, node: dict[str, Any]) -> "DbtModel":
        depends_on = node.get("depends_on", {}).get("nodes", [])
        columns = {
            col_name: {
                "name": col_info.get("name", col_name),
                "description": col_info.get("description", ""),
                "data_type": col_info.get("data_type"),
                "meta": col_info.get("meta", {}),
            }
            for col_name, col_info in node.get("columns", {}).items()
        }
        return cls(
            unique_id=node.get("unique_id", ""),
            name=node.get("name", ""),
            database=node.get("database"),
            schema=node.get("schema", ""),
            description=node.get("description"),
            resource_type=node.get("resource_type", "model"),
            package_name=node.get("package_name", ""),
            path=node.get("path", ""),
            original_file_path=node.get("original_file_path", ""),
            depends_on=depends_on,
            tags=node.get("tags", []),
            meta=node.get("meta", {}),
            columns=columns,
            config=node.get("config", {}),
        )


@dataclass
class DbtManifest:
    """Represents a parsed dbt manifest.json."""

    models: dict[str, DbtModel]
    sources: dict[str, dict[str, Any]]
    metadata: dict[str, Any]
    dbt_version: str

    @property
    def model_count(self) -> int:
        return len(self.models)

    def get_model(self, unique_id: str) -> Optional[DbtModel]:
        return self.models.get(unique_id)

    def get_model_by_name(self, name: str) -> Optional[DbtModel]:
        for model in self.models.values():
            if model.name == name:
                return model
        return None

    def get_upstream_models(self, model: DbtModel) -> list[DbtModel]:
        return [
            dep_model
            for dep_id in model.depends_on
            if dep_id.startswith("model.")
            and (dep_model := self.models.get(dep_id)) is not None
        ]

    def get_all_model_names(self) -> list[str]:
        return [model.name for model in self.models.values()]
