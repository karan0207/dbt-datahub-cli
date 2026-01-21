"""Configuration loader for governance rules."""

import logging
import os
from pathlib import Path
from typing import Any, Optional

import yaml

from dbt_datahub_governance.constants import (
    DEFAULT_CONFIG_FILENAME,
    ALTERNATE_CONFIG_FILENAMES,
)
from dbt_datahub_governance.exceptions import ConfigLoadError
from dbt_datahub_governance.models.governance import GovernanceConfig

logger = logging.getLogger(__name__)


def find_config_file(
    start_path: Optional[str | Path] = None,
    config_filename: Optional[str] = None,
) -> Optional[Path]:
    """Find a governance configuration file by searching up the directory tree."""
    start = Path(start_path) if start_path else Path.cwd()

    if config_filename:
        explicit_path = start / config_filename if start.is_dir() else Path(config_filename)
        if explicit_path.exists():
            return explicit_path
        if Path(config_filename).exists():
            return Path(config_filename)

    filenames_to_check = [DEFAULT_CONFIG_FILENAME] + ALTERNATE_CONFIG_FILENAMES
    current = start if start.is_dir() else start.parent

    while current != current.parent:
        for filename in filenames_to_check:
            config_path = current / filename
            if config_path.exists():
                logger.debug(f"Found config file: {config_path}")
                return config_path
        current = current.parent

    for filename in filenames_to_check:
        config_path = current / filename
        if config_path.exists():
            return config_path

    return None


def load_config_from_file(config_path: str | Path) -> GovernanceConfig:
    """Load governance configuration from a YAML file."""
    path = Path(config_path)

    if not path.exists():
        raise ConfigLoadError(f"Configuration file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigLoadError(f"Invalid YAML in configuration file: {e}")
    except IOError as e:
        raise ConfigLoadError(f"Error reading configuration file: {e}")

    if data is None:
        raise ConfigLoadError("Configuration file is empty")
    if not isinstance(data, dict):
        raise ConfigLoadError("Configuration file must contain a YAML dictionary")

    return GovernanceConfig.from_dict(data)


def load_config(
    config_path: Optional[str | Path] = None,
    start_path: Optional[str | Path] = None,
) -> GovernanceConfig:
    """Load governance configuration from file or return defaults."""
    if config_path:
        return load_config_from_file(config_path)

    found_config = find_config_file(start_path)
    if found_config:
        logger.info(f"Using configuration file: {found_config}")
        return load_config_from_file(found_config)

    logger.info("No configuration file found, using defaults")
    return GovernanceConfig.default()


def create_default_config_file(
    output_path: Optional[str | Path] = None,
    include_comments: bool = True,
) -> Path:
    """Create a default governance configuration file."""
    path = Path(output_path) if output_path else Path.cwd() / DEFAULT_CONFIG_FILENAME

    if include_comments:
        content = '''# dbt-datahub-governance Configuration
# This file configures governance validation rules for your dbt project.

# Target data platform (snowflake, bigquery, redshift, postgres, databricks)
target_platform: snowflake

# DataHub environment (typically PROD, DEV, or STAGING)
environment: PROD

# Platform instance (optional, for multi-account setups)
# platform_instance: my-account

# Whether to fail the validation if there are warnings
fail_on_warnings: false

# Patterns to include/exclude models
include_patterns:
  - "*"  # Include all models by default

exclude_patterns:
  - "staging_*"  # Example: exclude staging models
  - "tmp_*"      # Example: exclude temporary models

# Governance rules configuration
rules:
  # Require all models to have an owner in DataHub
  require_owner:
    enabled: true
    severity: error  # error, warning, or info
    description: "All models must have an owner assigned in DataHub"

  # Require all models to have a description
  require_description:
    enabled: true
    severity: error
    description: "All models must have a description in dbt or DataHub"

  # Require all models to be assigned to a domain
  require_domain:
    enabled: false
    severity: warning
    description: "All models should be assigned to a domain in DataHub"

  # Prevent models from depending on deprecated datasets
  no_deprecated_upstream:
    enabled: true
    severity: error
    description: "Models cannot depend on deprecated upstream datasets"

  # Require upstream dependencies to have owners
  upstream_must_have_owner:
    enabled: true
    severity: warning
    description: "Upstream dependencies should have owners"

  # Require tags on models
  require_tags:
    enabled: false
    severity: warning
    description: "All models should have tags assigned"
'''
    else:
        config = GovernanceConfig.default()
        content = yaml.dump(
            {
                "target_platform": config.target_platform,
                "environment": config.environment,
                "platform_instance": config.platform_instance,
                "fail_on_warnings": config.fail_on_warnings,
                "include_patterns": config.include_patterns,
                "exclude_patterns": config.exclude_patterns,
                "rules": {
                    name: {
                        "enabled": rule.enabled,
                        "severity": rule.severity.value,
                        "description": rule.description,
                    }
                    for name, rule in config.rules.items()
                },
            },
            default_flow_style=False,
            sort_keys=False,
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    logger.info(f"Created default configuration file: {path}")
    return path


def get_datahub_connection_from_env() -> dict[str, Any]:
    """Get DataHub connection settings from environment variables."""
    server = os.environ.get("DATAHUB_GMS_URL") or os.environ.get("DATAHUB_SERVER")
    token = os.environ.get("DATAHUB_GMS_TOKEN") or os.environ.get("DATAHUB_TOKEN")

    if not server:
        raise ConfigLoadError(
            "DataHub server URL not configured. "
            "Set DATAHUB_GMS_URL or DATAHUB_SERVER environment variable."
        )

    return {"server": server, "token": token}
