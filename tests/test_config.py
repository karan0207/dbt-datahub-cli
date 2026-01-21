"""Tests for configuration loading."""

import tempfile
from pathlib import Path

import pytest
import yaml

from dbt_datahub_governance.config import (
    ConfigLoadError,
    create_default_config_file,
    find_config_file,
    get_datahub_connection_from_env,
    load_config,
    load_config_from_file,
)
from dbt_datahub_governance.models.governance import GovernanceConfig, ValidationSeverity


class TestFindConfigFile:
    """Tests for find_config_file function."""

    def test_find_explicit_file(self, sample_config_file: Path) -> None:
        """Test finding explicitly specified config file."""
        found = find_config_file(
            start_path=sample_config_file.parent,
            config_filename=sample_config_file.name,
        )
        assert found == sample_config_file

    def test_find_default_filename(self) -> None:
        """Test finding config file with default name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "governance.yml"
            config_path.write_text("rules: {}")

            found = find_config_file(start_path=tmpdir)
            assert found == config_path

    def test_find_alternate_filename(self) -> None:
        """Test finding config file with alternate name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / ".governance.yaml"
            config_path.write_text("rules: {}")

            found = find_config_file(start_path=tmpdir)
            assert found == config_path

    def test_not_found(self) -> None:
        """Test when no config file is found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            found = find_config_file(start_path=tmpdir)
            assert found is None


class TestLoadConfigFromFile:
    """Tests for load_config_from_file function."""

    def test_load_valid_config(self, sample_config_file: Path) -> None:
        """Test loading valid configuration file."""
        config = load_config_from_file(sample_config_file)

        assert config.target_platform == "snowflake"
        assert config.environment == "PROD"
        assert "require_owner" in config.rules
        assert config.rules["require_owner"].enabled is True

    def test_load_nonexistent_file(self) -> None:
        """Test loading non-existent file raises error."""
        with pytest.raises(ConfigLoadError, match="not found"):
            load_config_from_file("/nonexistent/config.yml")

    def test_load_invalid_yaml(self) -> None:
        """Test loading invalid YAML raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Invalid YAML"):
                load_config_from_file(temp_path)
        finally:
            temp_path.unlink()

    def test_load_empty_file(self) -> None:
        """Test loading empty file raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            temp_path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="empty"):
                load_config_from_file(temp_path)
        finally:
            temp_path.unlink()


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_with_explicit_path(self, sample_config_file: Path) -> None:
        """Test loading with explicit path."""
        config = load_config(config_path=sample_config_file)

        assert config.target_platform == "snowflake"

    def test_load_default_when_not_found(self) -> None:
        """Test loading default config when no file found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = load_config(start_path=tmpdir)

            # Should return default config
            assert config.target_platform == "snowflake"
            assert "require_owner" in config.rules


class TestGovernanceConfigFromDict:
    """Tests for GovernanceConfig.from_dict method."""

    def test_from_dict_basic(self) -> None:
        """Test creating config from basic dictionary."""
        data = {
            "target_platform": "bigquery",
            "environment": "DEV",
            "rules": {
                "require_owner": True,
                "require_description": False,
            },
        }

        config = GovernanceConfig.from_dict(data)

        assert config.target_platform == "bigquery"
        assert config.environment == "DEV"
        assert config.rules["require_owner"].enabled is True
        assert config.rules["require_description"].enabled is False

    def test_from_dict_with_severity(self) -> None:
        """Test creating config with severity settings."""
        data = {
            "rules": {
                "require_owner": {
                    "enabled": True,
                    "severity": "warning",
                },
            },
        }

        config = GovernanceConfig.from_dict(data)

        assert config.rules["require_owner"].severity == ValidationSeverity.WARNING

    def test_from_dict_with_patterns(self) -> None:
        """Test creating config with include/exclude patterns."""
        data = {
            "include_patterns": ["marts_*", "dim_*"],
            "exclude_patterns": ["staging_*"],
            "rules": {},
        }

        config = GovernanceConfig.from_dict(data)

        assert "marts_*" in config.include_patterns
        assert "staging_*" in config.exclude_patterns


class TestCreateDefaultConfigFile:
    """Tests for create_default_config_file function."""

    def test_create_with_comments(self) -> None:
        """Test creating config file with comments."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "governance.yml"
            created_path = create_default_config_file(output_path, include_comments=True)

            assert created_path.exists()

            content = created_path.read_text()
            assert "# dbt-datahub-governance" in content
            assert "require_owner" in content

    def test_create_without_comments(self) -> None:
        """Test creating config file without comments."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "governance.yml"
            created_path = create_default_config_file(output_path, include_comments=False)

            assert created_path.exists()

            # Should be valid YAML
            with open(created_path) as f:
                data = yaml.safe_load(f)

            assert "rules" in data


class TestGetDatahubConnectionFromEnv:
    """Tests for get_datahub_connection_from_env function."""

    def test_get_connection_with_gms_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting connection from DATAHUB_GMS_URL."""
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "test-token")

        conn = get_datahub_connection_from_env()

        assert conn["server"] == "http://localhost:8080"
        assert conn["token"] == "test-token"

    def test_get_connection_with_server(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting connection from DATAHUB_SERVER."""
        monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
        monkeypatch.setenv("DATAHUB_SERVER", "http://datahub:8080")

        conn = get_datahub_connection_from_env()

        assert conn["server"] == "http://datahub:8080"

    def test_get_connection_missing_server(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test error when server URL not configured."""
        monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
        monkeypatch.delenv("DATAHUB_SERVER", raising=False)

        with pytest.raises(ConfigLoadError, match="server URL not configured"):
            get_datahub_connection_from_env()
