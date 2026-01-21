"""Test configuration for pytest."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Generator

import pytest

from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.models.governance import (
    DatasetGovernanceStatus,
    GovernanceConfig,
    GovernanceRule,
    ValidationSeverity,
)


@pytest.fixture
def sample_manifest_data() -> dict[str, Any]:
    """Return sample dbt manifest data."""
    return {
        "metadata": {
            "dbt_version": "1.7.0",
            "project_name": "test_project",
            "generated_at": "2024-01-01T00:00:00Z",
        },
        "nodes": {
            "model.test_project.dim_customers": {
                "unique_id": "model.test_project.dim_customers",
                "name": "dim_customers",
                "database": "ANALYTICS_DB",
                "schema": "MARTS",
                "description": "Customer dimension table",
                "resource_type": "model",
                "package_name": "test_project",
                "path": "marts/dim_customers.sql",
                "original_file_path": "models/marts/dim_customers.sql",
                "depends_on": {"nodes": ["model.test_project.stg_customers"]},
                "tags": ["pii", "core"],
                "meta": {"owner": "data-team"},
                "columns": {
                    "customer_id": {
                        "name": "customer_id",
                        "description": "Primary key",
                        "data_type": "INTEGER",
                    },
                    "customer_name": {
                        "name": "customer_name",
                        "description": "Customer full name",
                        "data_type": "VARCHAR",
                    },
                },
                "config": {"materialized": "table"},
            },
            "model.test_project.stg_customers": {
                "unique_id": "model.test_project.stg_customers",
                "name": "stg_customers",
                "database": "ANALYTICS_DB",
                "schema": "STAGING",
                "description": "",
                "resource_type": "model",
                "package_name": "test_project",
                "path": "staging/stg_customers.sql",
                "original_file_path": "models/staging/stg_customers.sql",
                "depends_on": {"nodes": []},
                "tags": [],
                "meta": {},
                "columns": {},
                "config": {"materialized": "view"},
            },
            "model.test_project.fct_orders": {
                "unique_id": "model.test_project.fct_orders",
                "name": "fct_orders",
                "database": "ANALYTICS_DB",
                "schema": "MARTS",
                "description": "Orders fact table containing all orders",
                "resource_type": "model",
                "package_name": "test_project",
                "path": "marts/fct_orders.sql",
                "original_file_path": "models/marts/fct_orders.sql",
                "depends_on": {
                    "nodes": [
                        "model.test_project.stg_orders",
                        "model.test_project.dim_customers",
                    ]
                },
                "tags": ["core"],
                "meta": {},
                "columns": {},
                "config": {"materialized": "table"},
            },
            "model.test_project.stg_orders": {
                "unique_id": "model.test_project.stg_orders",
                "name": "stg_orders",
                "database": "ANALYTICS_DB",
                "schema": "STAGING",
                "description": "Staged orders data",
                "resource_type": "model",
                "package_name": "test_project",
                "path": "staging/stg_orders.sql",
                "original_file_path": "models/staging/stg_orders.sql",
                "depends_on": {"nodes": []},
                "tags": [],
                "meta": {},
                "columns": {},
                "config": {"materialized": "view"},
            },
        },
        "sources": {
            "source.test_project.raw.customers": {
                "unique_id": "source.test_project.raw.customers",
                "name": "customers",
                "database": "RAW_DB",
                "schema": "RAW",
                "description": "Raw customer data",
                "resource_type": "source",
            }
        },
    }


@pytest.fixture
def sample_manifest_file(sample_manifest_data: dict[str, Any]) -> Generator[Path, None, None]:
    """Create a temporary manifest.json file."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(sample_manifest_data, f)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_dbt_model() -> DbtModel:
    """Return a sample DbtModel instance."""
    return DbtModel(
        unique_id="model.test_project.dim_customers",
        name="dim_customers",
        database="ANALYTICS_DB",
        schema="MARTS",
        description="Customer dimension table",
        resource_type="model",
        package_name="test_project",
        path="marts/dim_customers.sql",
        original_file_path="models/marts/dim_customers.sql",
        depends_on=["model.test_project.stg_customers"],
        tags=["pii", "core"],
        meta={"owner": "data-team"},
        columns={
            "customer_id": {"name": "customer_id", "description": "Primary key"},
        },
        config={"materialized": "table"},
    )


@pytest.fixture
def sample_dbt_manifest(sample_dbt_model: DbtModel) -> DbtManifest:
    """Return a sample DbtManifest instance."""
    stg_model = DbtModel(
        unique_id="model.test_project.stg_customers",
        name="stg_customers",
        database="ANALYTICS_DB",
        schema="STAGING",
        description="",
        resource_type="model",
        package_name="test_project",
        path="staging/stg_customers.sql",
        original_file_path="models/staging/stg_customers.sql",
        depends_on=[],
        tags=[],
        meta={},
        columns={},
        config={"materialized": "view"},
    )

    return DbtManifest(
        models={
            sample_dbt_model.unique_id: sample_dbt_model,
            stg_model.unique_id: stg_model,
        },
        sources={},
        metadata={"dbt_version": "1.7.0"},
        dbt_version="1.7.0",
    )


@pytest.fixture
def sample_governance_status_with_owner() -> DatasetGovernanceStatus:
    """Return a governance status with owner."""
    return DatasetGovernanceStatus(
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS_DB.MARTS.DIM_CUSTOMERS,PROD)",
        exists=True,
        has_owner=True,
        has_description=True,
        has_domain=True,
        has_tags=True,
        is_deprecated=False,
        owners=["urn:li:corpuser:john.doe"],
        domain="urn:li:domain:analytics",
        tags=["urn:li:tag:pii"],
        description="Customer dimension table",
    )


@pytest.fixture
def sample_governance_status_without_owner() -> DatasetGovernanceStatus:
    """Return a governance status without owner."""
    return DatasetGovernanceStatus(
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS_DB.STAGING.STG_CUSTOMERS,PROD)",
        exists=True,
        has_owner=False,
        has_description=False,
        has_domain=False,
        has_tags=False,
        is_deprecated=False,
        owners=[],
        domain=None,
        tags=[],
        description=None,
    )


@pytest.fixture
def sample_deprecated_status() -> DatasetGovernanceStatus:
    """Return a deprecated dataset status."""
    return DatasetGovernanceStatus(
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS_DB.LEGACY.OLD_TABLE,PROD)",
        exists=True,
        has_owner=True,
        has_description=True,
        has_domain=False,
        has_tags=False,
        is_deprecated=True,
        owners=["urn:li:corpuser:jane.doe"],
        domain=None,
        tags=[],
        description="Legacy table - do not use",
        deprecation_note="Deprecated: Use new_table instead",
    )


@pytest.fixture
def sample_governance_config() -> GovernanceConfig:
    """Return a sample governance configuration."""
    return GovernanceConfig(
        rules={
            "require_owner": GovernanceRule(
                name="require_owner",
                enabled=True,
                severity=ValidationSeverity.ERROR,
            ),
            "require_description": GovernanceRule(
                name="require_description",
                enabled=True,
                severity=ValidationSeverity.ERROR,
            ),
            "require_domain": GovernanceRule(
                name="require_domain",
                enabled=False,
                severity=ValidationSeverity.WARNING,
            ),
            "no_deprecated_upstream": GovernanceRule(
                name="no_deprecated_upstream",
                enabled=True,
                severity=ValidationSeverity.ERROR,
            ),
        },
        target_platform="snowflake",
        environment="PROD",
        include_patterns=["*"],
        exclude_patterns=[],
    )


@pytest.fixture
def sample_config_yaml() -> str:
    """Return sample configuration YAML content."""
    return """
target_platform: snowflake
environment: PROD
fail_on_warnings: false

include_patterns:
  - "*"

exclude_patterns:
  - "staging_*"

rules:
  require_owner:
    enabled: true
    severity: error
    description: "All models must have an owner"

  require_description:
    enabled: true
    severity: warning

  require_domain:
    enabled: false
    severity: warning
"""


@pytest.fixture
def sample_config_file(sample_config_yaml: str) -> Generator[Path, None, None]:
    """Create a temporary configuration file."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    ) as f:
        f.write(sample_config_yaml)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()
