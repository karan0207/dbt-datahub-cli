"""URN mapping utilities for converting dbt models to DataHub URNs."""

import logging
import re
from typing import Optional

from dbt_datahub_governance.models.dbt_models import DbtModel

logger = logging.getLogger(__name__)


class UrnMapper:
    """Maps dbt models to DataHub dataset URNs."""

    UPPERCASE_PLATFORMS = {"snowflake"}
    LOWERCASE_PLATFORMS = {"postgres", "mysql"}

    def __init__(
        self,
        platform: str,
        env: str = "PROD",
        platform_instance: Optional[str] = None,
        custom_database: Optional[str] = None,
        custom_schema: Optional[str] = None,
    ) -> None:
        self.platform = platform.lower()
        self.env = env
        self.platform_instance = platform_instance
        self.custom_database = custom_database
        self.custom_schema = custom_schema

    def _normalize_name(self, name: str) -> str:
        if self.platform in self.UPPERCASE_PLATFORMS:
            return name.upper()
        elif self.platform in self.LOWERCASE_PLATFORMS:
            return name.lower()
        return name

    def get_dataset_name(self, model: DbtModel) -> str:
        """Get the full dataset name for a dbt model."""
        database = self.custom_database or model.database or ""
        schema = self.custom_schema or model.schema
        name = model.name

        database = self._normalize_name(database) if database else ""
        schema = self._normalize_name(schema)
        name = self._normalize_name(name)

        if self.platform == "bigquery":
            return f"{database}.{schema}.{name}"
        elif self.platform in ["snowflake", "redshift", "postgres", "databricks"]:
            if database:
                return f"{database}.{schema}.{name}"
            return f"{schema}.{name}"
        else:
            if database:
                return f"{database}.{schema}.{name}"
            return f"{schema}.{name}"

    def model_to_urn(self, model: DbtModel) -> str:
        """Convert a dbt model to a DataHub dataset URN."""
        dataset_name = self.get_dataset_name(model)
        return self.build_urn(dataset_name)

    def build_urn(self, dataset_name: str) -> str:
        """Build a DataHub URN from a dataset name."""
        try:
            from datahub.emitter.mce_builder import (
                make_dataset_urn,
                make_dataset_urn_with_platform_instance,
            )

            if self.platform_instance:
                return make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=dataset_name,
                    platform_instance=self.platform_instance,
                    env=self.env,
                )
            return make_dataset_urn(platform=self.platform, name=dataset_name, env=self.env)
        except ImportError:
            return self._build_urn_manual(dataset_name)

    def _build_urn_manual(self, dataset_name: str) -> str:
        """Build URN manually without SDK."""
        if self.platform_instance:
            return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{self.platform_instance}.{dataset_name},{self.env})"
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.env})"

    def source_to_urn(
        self,
        source_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
    ) -> str:
        """Convert a dbt source to a DataHub dataset URN."""
        database = source_database or self.custom_database or ""
        schema = source_schema or self.custom_schema or ""

        database = self._normalize_name(database) if database else ""
        schema = self._normalize_name(schema) if schema else ""
        name = self._normalize_name(source_name)

        if database and schema:
            dataset_name = f"{database}.{schema}.{name}"
        elif schema:
            dataset_name = f"{schema}.{name}"
        else:
            dataset_name = name

        return self.build_urn(dataset_name)


def parse_urn(urn: str) -> dict[str, str]:
    """Parse a DataHub URN into its components."""
    pattern = r"urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)"
    match = re.match(pattern, urn)

    if match:
        return {
            "platform": match.group(1),
            "name": match.group(2),
            "env": match.group(3),
        }
    return {"platform": "", "name": "", "env": ""}
