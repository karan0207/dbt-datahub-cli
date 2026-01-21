"""Tests for governance rules and validation engine."""

import pytest

from dbt_datahub_governance.datahub import MockDataHubClient
from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.models.governance import (
    DatasetGovernanceStatus,
    GovernanceConfig,
    GovernanceRule,
    ValidationReport,
    ValidationResult,
    ValidationSeverity,
)
from dbt_datahub_governance.rules import (
    GovernanceEngine,
    NoDeprecatedUpstreamRule,
    RequireDescriptionRule,
    RequireDomainRule,
    RequireOwnerRule,
    RequireTagsRule,
    UpstreamMustHaveOwnerRule,
)


class TestRequireOwnerRule:
    """Tests for RequireOwnerRule."""

    def test_passes_when_has_owner(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule passes when dataset has owner."""
        rule = RequireOwnerRule(
            GovernanceRule(name="require_owner", enabled=True, severity=ValidationSeverity.ERROR)
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_with_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True
        assert result.rule_name == "require_owner"

    def test_fails_when_no_owner(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_without_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule fails when dataset has no owner."""
        rule = RequireOwnerRule(
            GovernanceRule(name="require_owner", enabled=True, severity=ValidationSeverity.ERROR)
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_without_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is False
        assert "owner" in result.message.lower()

    def test_fails_when_dataset_not_found(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
    ) -> None:
        """Test rule fails when dataset not found in DataHub."""
        rule = RequireOwnerRule(
            GovernanceRule(name="require_owner", enabled=True, severity=ValidationSeverity.ERROR)
        )

        not_found_status = DatasetGovernanceStatus(
            urn="urn:li:dataset:notfound", exists=False
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=not_found_status,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is False
        assert "not found" in result.message.lower()


class TestRequireDescriptionRule:
    """Tests for RequireDescriptionRule."""

    def test_passes_with_dbt_description(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_without_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule passes when model has dbt description."""
        rule = RequireDescriptionRule(
            GovernanceRule(name="require_description", enabled=True, severity=ValidationSeverity.ERROR)
        )

        # sample_dbt_model has a description
        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_without_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True
        assert "dbt" in result.details.get("source", "").lower()

    def test_passes_with_datahub_description(
        self,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule passes when DataHub has description."""
        rule = RequireDescriptionRule(
            GovernanceRule(name="require_description", enabled=True, severity=ValidationSeverity.ERROR)
        )

        # Create model without description
        model = DbtModel(
            unique_id="model.test.no_desc",
            name="no_desc",
            database="DB",
            schema="SCHEMA",
            description="",  # Empty description
            resource_type="model",
            package_name="test",
            path="test.sql",
            original_file_path="models/test.sql",
        )

        result = rule.validate(
            model=model,
            status=sample_governance_status_with_owner,  # Has description
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True
        assert "datahub" in result.details.get("source", "").lower()

    def test_fails_without_any_description(
        self,
        sample_dbt_manifest: DbtManifest,
    ) -> None:
        """Test rule fails when no description anywhere."""
        rule = RequireDescriptionRule(
            GovernanceRule(name="require_description", enabled=True, severity=ValidationSeverity.ERROR)
        )

        model = DbtModel(
            unique_id="model.test.no_desc",
            name="no_desc",
            database="DB",
            schema="SCHEMA",
            description="",
            resource_type="model",
            package_name="test",
            path="test.sql",
            original_file_path="models/test.sql",
        )

        status = DatasetGovernanceStatus(
            urn="urn:li:dataset:test",
            exists=True,
            has_description=False,
        )

        result = rule.validate(
            model=model,
            status=status,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is False
        assert "description" in result.message.lower()


class TestRequireDomainRule:
    """Tests for RequireDomainRule."""

    def test_passes_with_domain(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule passes when dataset has domain."""
        rule = RequireDomainRule(
            GovernanceRule(name="require_domain", enabled=True, severity=ValidationSeverity.WARNING)
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_with_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True

    def test_fails_without_domain(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_without_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule fails when dataset has no domain."""
        rule = RequireDomainRule(
            GovernanceRule(name="require_domain", enabled=True, severity=ValidationSeverity.WARNING)
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_without_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is False
        assert "domain" in result.message.lower()


class TestRequireTagsRule:
    """Tests for RequireTagsRule."""

    def test_passes_with_dbt_tags(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
    ) -> None:
        """Test rule passes when model has dbt tags."""
        rule = RequireTagsRule(
            GovernanceRule(name="require_tags", enabled=True, severity=ValidationSeverity.WARNING)
        )

        status = DatasetGovernanceStatus(
            urn="urn:li:dataset:test",
            exists=True,
            has_tags=False,
        )

        # sample_dbt_model has tags
        result = rule.validate(
            model=sample_dbt_model,
            status=status,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True
        assert "dbt" in result.details.get("source", "").lower()


class TestNoDeprecatedUpstreamRule:
    """Tests for NoDeprecatedUpstreamRule."""

    def test_passes_with_no_deprecated_upstream(
        self,
        sample_dbt_model: DbtModel,
        sample_dbt_manifest: DbtManifest,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test rule passes when no upstream is deprecated."""
        rule = NoDeprecatedUpstreamRule(
            GovernanceRule(name="no_deprecated_upstream", enabled=True, severity=ValidationSeverity.ERROR)
        )

        result = rule.validate(
            model=sample_dbt_model,
            status=sample_governance_status_with_owner,
            manifest=sample_dbt_manifest,
            all_statuses={},
        )

        assert result.passed is True


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_is_error(self) -> None:
        """Test is_error property."""
        result = ValidationResult(
            rule_name="test",
            model_name="test_model",
            model_unique_id="model.test.test_model",
            passed=False,
            severity=ValidationSeverity.ERROR,
            message="Test error",
        )

        assert result.is_error is True
        assert result.is_warning is False

    def test_is_warning(self) -> None:
        """Test is_warning property."""
        result = ValidationResult(
            rule_name="test",
            model_name="test_model",
            model_unique_id="model.test.test_model",
            passed=False,
            severity=ValidationSeverity.WARNING,
            message="Test warning",
        )

        assert result.is_error is False
        assert result.is_warning is True

    def test_passed_result(self) -> None:
        """Test passed result."""
        result = ValidationResult(
            rule_name="test",
            model_name="test_model",
            model_unique_id="model.test.test_model",
            passed=True,
            severity=ValidationSeverity.ERROR,
            message="Test passed",
        )

        assert result.is_error is False
        assert result.is_warning is False


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_add_result(self) -> None:
        """Test adding results to report."""
        report = ValidationReport()

        passed = ValidationResult(
            rule_name="test",
            model_name="model1",
            model_unique_id="model.test.model1",
            passed=True,
            severity=ValidationSeverity.ERROR,
            message="Passed",
        )
        report.add_result(passed)

        assert report.total_checks == 1
        assert report.passed == 1
        assert report.errors == 0

    def test_count_errors(self) -> None:
        """Test error counting."""
        report = ValidationReport()

        error = ValidationResult(
            rule_name="test",
            model_name="model1",
            model_unique_id="model.test.model1",
            passed=False,
            severity=ValidationSeverity.ERROR,
            message="Error",
        )
        report.add_result(error)

        assert report.errors == 1
        assert report.has_errors is True
        assert report.is_successful is False

    def test_count_warnings(self) -> None:
        """Test warning counting."""
        report = ValidationReport()

        warning = ValidationResult(
            rule_name="test",
            model_name="model1",
            model_unique_id="model.test.model1",
            passed=False,
            severity=ValidationSeverity.WARNING,
            message="Warning",
        )
        report.add_result(warning)

        assert report.warnings == 1
        assert report.has_warnings is True
        # Warnings don't affect success status by default
        assert report.is_successful is True

    def test_get_errors(self) -> None:
        """Test getting error results."""
        report = ValidationReport()

        error = ValidationResult(
            rule_name="test",
            model_name="model1",
            model_unique_id="model.test.model1",
            passed=False,
            severity=ValidationSeverity.ERROR,
            message="Error",
        )
        warning = ValidationResult(
            rule_name="test",
            model_name="model2",
            model_unique_id="model.test.model2",
            passed=False,
            severity=ValidationSeverity.WARNING,
            message="Warning",
        )
        report.add_result(error)
        report.add_result(warning)

        errors = report.get_errors()
        assert len(errors) == 1
        assert errors[0].model_name == "model1"

    def test_to_dict(self) -> None:
        """Test converting report to dictionary."""
        report = ValidationReport()
        report.total_models_checked = 2

        result = ValidationResult(
            rule_name="test",
            model_name="model1",
            model_unique_id="model.test.model1",
            passed=True,
            severity=ValidationSeverity.ERROR,
            message="Passed",
        )
        report.add_result(result)

        data = report.to_dict()

        assert "summary" in data
        assert "results" in data
        assert data["summary"]["total_models_checked"] == 2
        assert data["summary"]["passed"] == 1


class TestGovernanceEngine:
    """Tests for GovernanceEngine."""

    def test_validate_all_models(
        self,
        sample_dbt_manifest: DbtManifest,
        sample_governance_config: GovernanceConfig,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test validating all models."""
        # Set up mock client with data for all models
        mock_data = {}
        for model in sample_dbt_manifest.models.values():
            urn = f"urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS_DB.{model.schema.upper()}.{model.name.upper()},PROD)"
            mock_data[urn] = DatasetGovernanceStatus(
                urn=urn,
                exists=True,
                has_owner=True,
                has_description=bool(model.description),
                owners=["urn:li:corpuser:test"],
            )

        client = MockDataHubClient(mock_data=mock_data)

        engine = GovernanceEngine(
            config=sample_governance_config,
            datahub_client=client,
            manifest=sample_dbt_manifest,
        )

        report = engine.validate()

        assert report.total_models_checked == 2  # From sample_dbt_manifest
        assert report.total_checks > 0

    def test_validate_single_model(
        self,
        sample_dbt_manifest: DbtManifest,
        sample_governance_config: GovernanceConfig,
    ) -> None:
        """Test validating a single model."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS_DB.MARTS.DIM_CUSTOMERS,PROD)"
        mock_data = {
            urn: DatasetGovernanceStatus(
                urn=urn,
                exists=True,
                has_owner=True,
                has_description=True,
                owners=["urn:li:corpuser:test"],
            )
        }

        client = MockDataHubClient(mock_data=mock_data)

        engine = GovernanceEngine(
            config=sample_governance_config,
            datahub_client=client,
            manifest=sample_dbt_manifest,
        )

        report = engine.validate_single_model("dim_customers")

        assert report.total_models_checked == 1

    def test_validate_single_model_not_found(
        self,
        sample_dbt_manifest: DbtManifest,
        sample_governance_config: GovernanceConfig,
    ) -> None:
        """Test validating a non-existent model."""
        client = MockDataHubClient()

        engine = GovernanceEngine(
            config=sample_governance_config,
            datahub_client=client,
            manifest=sample_dbt_manifest,
        )

        report = engine.validate_single_model("nonexistent_model")

        assert report.has_errors is True

    def test_exclude_patterns(
        self,
        sample_governance_config: GovernanceConfig,
    ) -> None:
        """Test that exclude patterns work."""
        # Create manifest with staging model
        manifest = DbtManifest(
            models={
                "model.test.staging_users": DbtModel(
                    unique_id="model.test.staging_users",
                    name="staging_users",
                    database="DB",
                    schema="STAGING",
                    description="",
                    resource_type="model",
                    package_name="test",
                    path="staging/staging_users.sql",
                    original_file_path="models/staging/staging_users.sql",
                ),
            },
            sources={},
            metadata={},
            dbt_version="1.7.0",
        )

        # Add exclude pattern
        config = GovernanceConfig(
            rules=sample_governance_config.rules,
            target_platform="snowflake",
            environment="PROD",
            include_patterns=["*"],
            exclude_patterns=["staging_*"],
        )

        client = MockDataHubClient()
        engine = GovernanceEngine(
            config=config,
            datahub_client=client,
            manifest=manifest,
        )

        report = engine.validate()

        # staging_users should be excluded
        assert report.total_models_checked == 0
