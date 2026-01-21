"""Tests for reporters."""

import io
import json

import pytest

from dbt_datahub_governance.models.governance import (
    ValidationReport,
    ValidationResult,
    ValidationSeverity,
)
from dbt_datahub_governance.reporters import (
    ConsoleReporter,
    GithubActionsReporter,
    JsonReporter,
    MarkdownReporter,
    get_reporter,
)


@pytest.fixture
def sample_report() -> ValidationReport:
    """Create a sample validation report."""
    report = ValidationReport()
    report.total_models_checked = 3

    # Add passed result
    report.add_result(
        ValidationResult(
            rule_name="require_owner",
            model_name="dim_customers",
            model_unique_id="model.test.dim_customers",
            passed=True,
            severity=ValidationSeverity.ERROR,
            message="Model has owner",
        )
    )

    # Add error result
    report.add_result(
        ValidationResult(
            rule_name="require_owner",
            model_name="stg_orders",
            model_unique_id="model.test.stg_orders",
            passed=False,
            severity=ValidationSeverity.ERROR,
            message="Missing owner",
        )
    )

    # Add warning result
    report.add_result(
        ValidationResult(
            rule_name="require_domain",
            model_name="fct_orders",
            model_unique_id="model.test.fct_orders",
            passed=False,
            severity=ValidationSeverity.WARNING,
            message="Missing domain assignment",
        )
    )

    return report


class TestConsoleReporter:
    """Tests for ConsoleReporter."""

    def test_report_output(self, sample_report: ValidationReport) -> None:
        """Test console reporter output."""
        output = io.StringIO()
        reporter = ConsoleReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "Validation Report" in result
        assert "Models Checked" in result
        assert "Errors" in result
        assert "Warnings" in result

    def test_report_shows_errors(self, sample_report: ValidationReport) -> None:
        """Test that errors are shown."""
        output = io.StringIO()
        reporter = ConsoleReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "stg_orders" in result
        assert "Missing owner" in result

    def test_report_shows_warnings(self, sample_report: ValidationReport) -> None:
        """Test that warnings are shown."""
        output = io.StringIO()
        reporter = ConsoleReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "fct_orders" in result
        assert "domain" in result.lower()

    def test_show_passed_option(self, sample_report: ValidationReport) -> None:
        """Test show_passed option."""
        output = io.StringIO()
        reporter = ConsoleReporter(output=output, show_passed=True)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "dim_customers" in result
        assert "Passed" in result


class TestJsonReporter:
    """Tests for JsonReporter."""

    def test_json_output(self, sample_report: ValidationReport) -> None:
        """Test JSON reporter output."""
        output = io.StringIO()
        reporter = JsonReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()
        data = json.loads(result)

        assert "summary" in data
        assert "results" in data
        assert data["summary"]["total_models_checked"] == 3
        assert data["summary"]["errors"] == 1
        assert data["summary"]["warnings"] == 1

    def test_json_pretty_output(self, sample_report: ValidationReport) -> None:
        """Test pretty-printed JSON output."""
        output = io.StringIO()
        reporter = JsonReporter(output=output, pretty=True)
        reporter.report(sample_report)

        result = output.getvalue()

        # Pretty printed JSON should have newlines
        assert "\n" in result

    def test_json_compact_output(self, sample_report: ValidationReport) -> None:
        """Test compact JSON output."""
        output = io.StringIO()
        reporter = JsonReporter(output=output, pretty=False)
        reporter.report(sample_report)

        result = output.getvalue().strip()

        # Should be a single line
        lines = result.split("\n")
        assert len(lines) == 1


class TestMarkdownReporter:
    """Tests for MarkdownReporter."""

    def test_markdown_output(self, sample_report: ValidationReport) -> None:
        """Test Markdown reporter output."""
        output = io.StringIO()
        reporter = MarkdownReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "# dbt-datahub-governance Validation Report" in result
        assert "## Summary" in result
        assert "## Errors" in result
        assert "## Warnings" in result

    def test_markdown_tables(self, sample_report: ValidationReport) -> None:
        """Test Markdown tables are generated."""
        output = io.StringIO()
        reporter = MarkdownReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        # Should have table headers
        assert "| Model | Rule | Message |" in result
        assert "|-------|------|---------|" in result

    def test_markdown_emoji(self, sample_report: ValidationReport) -> None:
        """Test Markdown uses emoji."""
        output = io.StringIO()
        reporter = MarkdownReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        # Should have status emoji
        assert "❌" in result  # Errors


class TestGithubActionsReporter:
    """Tests for GithubActionsReporter."""

    def test_github_actions_format(self, sample_report: ValidationReport) -> None:
        """Test GitHub Actions workflow commands."""
        output = io.StringIO()
        reporter = GithubActionsReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        # Should have workflow commands
        assert "::error" in result
        assert "::warning" in result

    def test_github_actions_titles(self, sample_report: ValidationReport) -> None:
        """Test GitHub Actions titles include model names."""
        output = io.StringIO()
        reporter = GithubActionsReporter(output=output)
        reporter.report(sample_report)

        result = output.getvalue()

        assert "stg_orders" in result
        assert "fct_orders" in result


class TestGetReporter:
    """Tests for get_reporter factory function."""

    def test_get_console_reporter(self) -> None:
        """Test getting console reporter."""
        reporter = get_reporter("console")
        assert isinstance(reporter, ConsoleReporter)

    def test_get_json_reporter(self) -> None:
        """Test getting JSON reporter."""
        reporter = get_reporter("json")
        assert isinstance(reporter, JsonReporter)

    def test_get_markdown_reporter(self) -> None:
        """Test getting Markdown reporter."""
        reporter = get_reporter("markdown")
        assert isinstance(reporter, MarkdownReporter)

    def test_get_github_reporter(self) -> None:
        """Test getting GitHub Actions reporter."""
        reporter = get_reporter("github")
        assert isinstance(reporter, GithubActionsReporter)

    def test_invalid_format(self) -> None:
        """Test invalid format raises error."""
        with pytest.raises(ValueError, match="Unknown format"):
            get_reporter("invalid")

    def test_reporter_options(self) -> None:
        """Test passing options to reporter."""
        output = io.StringIO()
        reporter = get_reporter(
            "console",
            verbose=True,
            show_passed=True,
            output=output,
        )

        assert isinstance(reporter, ConsoleReporter)
        assert reporter.verbose is True
        assert reporter.show_passed is True
