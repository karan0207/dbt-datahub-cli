"""Tests for CLI commands."""

import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from dbt_datahub_governance.cli import main


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI test runner."""
    return CliRunner()


class TestInitCommand:
    """Tests for the init command."""

    def test_init_creates_config(self, cli_runner: CliRunner) -> None:
        """Test init command creates config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "governance.yml"

            result = cli_runner.invoke(main, ["init", "-o", str(output_path)])

            assert result.exit_code == 0
            assert output_path.exists()
            assert "Created" in result.output

    def test_init_fails_if_exists(self, cli_runner: CliRunner) -> None:
        """Test init command fails if file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "governance.yml"
            output_path.write_text("existing content")

            result = cli_runner.invoke(main, ["init", "-o", str(output_path)])

            assert result.exit_code != 0
            assert "already exists" in result.output

    def test_init_force_overwrite(self, cli_runner: CliRunner) -> None:
        """Test init command with --force overwrites existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "governance.yml"
            output_path.write_text("existing content")

            result = cli_runner.invoke(main, ["init", "-o", str(output_path), "--force"])

            assert result.exit_code == 0
            assert "existing content" not in output_path.read_text()


class TestListModelsCommand:
    """Tests for the list-models command."""

    def test_list_models(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test list-models command."""
        result = cli_runner.invoke(
            main, ["list-models", "-m", str(sample_manifest_file)]
        )

        assert result.exit_code == 0
        assert "dim_customers" in result.output
        assert "stg_customers" in result.output

    def test_list_models_with_platform(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test list-models command with specific platform."""
        result = cli_runner.invoke(
            main, ["list-models", "-m", str(sample_manifest_file), "-p", "bigquery"]
        )

        assert result.exit_code == 0
        assert "bigquery" in result.output

    def test_list_models_invalid_manifest(self, cli_runner: CliRunner) -> None:
        """Test list-models command with invalid manifest."""
        result = cli_runner.invoke(
            main, ["list-models", "-m", "/nonexistent/manifest.json"]
        )

        assert result.exit_code != 0


class TestValidateCommand:
    """Tests for the validate command."""

    def test_validate_dry_run(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test validate command in dry-run mode."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", str(sample_manifest_file),
                "--dry-run",
            ],
        )

        # Should complete without DataHub connection
        assert result.exit_code in [0, 1]  # 0=pass, 1=validation failed

    def test_validate_with_config(
        self,
        cli_runner: CliRunner,
        sample_manifest_file: Path,
        sample_config_file: Path,
    ) -> None:
        """Test validate command with config file."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", str(sample_manifest_file),
                "-C", str(sample_config_file),
                "--dry-run",
            ],
        )

        assert result.exit_code in [0, 1]

    def test_validate_json_output(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test validate command with JSON output."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", str(sample_manifest_file),
                "--dry-run",
                "-f", "json",
            ],
        )

        assert result.exit_code in [0, 1]

        # Output should be valid JSON
        output = result.output.strip()
        data = json.loads(output)
        assert "summary" in data
        assert "results" in data

    def test_validate_single_model(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test validate command for a single model."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", str(sample_manifest_file),
                "--dry-run",
                "-M", "dim_customers",
            ],
        )

        assert result.exit_code in [0, 1]

    def test_validate_verbose(
        self, cli_runner: CliRunner, sample_manifest_file: Path
    ) -> None:
        """Test validate command with verbose output."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", str(sample_manifest_file),
                "--dry-run",
                "-v",
            ],
        )

        assert result.exit_code in [0, 1]

    def test_validate_missing_manifest(self, cli_runner: CliRunner) -> None:
        """Test validate command with missing manifest."""
        result = cli_runner.invoke(
            main,
            [
                "validate",
                "-m", "/nonexistent/manifest.json",
            ],
        )

        assert result.exit_code != 0


class TestTestConnectionCommand:
    """Tests for the test-connection command."""

    def test_test_connection_requires_server(self, cli_runner: CliRunner) -> None:
        """Test that test-connection requires server URL."""
        # Run without environment variables to ensure it requires the server option
        result = cli_runner.invoke(
            main,
            ["test-connection"],
            env={"DATAHUB_GMS_URL": "", "DATAHUB_GMS_TOKEN": ""},
        )

        # Should fail without server
        assert result.exit_code != 0


class TestVersionOption:
    """Tests for version option."""

    def test_version(self, cli_runner: CliRunner) -> None:
        """Test --version option."""
        result = cli_runner.invoke(main, ["--version"])

        assert result.exit_code == 0
        assert "dbt-datahub-governance" in result.output


class TestHelpOption:
    """Tests for help option."""

    def test_help(self, cli_runner: CliRunner) -> None:
        """Test --help option."""
        result = cli_runner.invoke(main, ["--help"])

        assert result.exit_code == 0
        assert "validate" in result.output
        assert "init" in result.output

    def test_validate_help(self, cli_runner: CliRunner) -> None:
        """Test validate --help."""
        result = cli_runner.invoke(main, ["validate", "--help"])

        assert result.exit_code == 0
        assert "--manifest" in result.output
        assert "--config" in result.output
