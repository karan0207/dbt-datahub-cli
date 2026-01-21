"""Governance rules engine for validating dbt models."""

import fnmatch
import logging

from dbt_datahub_governance.datahub import DataHubClient
from dbt_datahub_governance.datahub.urn_mapper import UrnMapper
from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.models.governance import (
    DatasetGovernanceStatus,
    GovernanceConfig,
    ValidationReport,
    ValidationResult,
    ValidationSeverity,
)
from dbt_datahub_governance.rules.base import BaseRule
from dbt_datahub_governance.rules.builtin import (
    MaxUpstreamDependenciesRule,
    NamingConventionRule,
    NoDeprecatedUpstreamRule,
    RequireColumnDescriptionsRule,
    RequireDescriptionRule,
    RequireDomainRule,
    RequireMaterializationRule,
    RequireOwnerRule,
    RequirePIITagRule,
    RequireTagsRule,
    UpstreamMustHaveOwnerRule,
)

logger = logging.getLogger(__name__)

RULE_REGISTRY: dict[str, type[BaseRule]] = {
    "require_owner": RequireOwnerRule,
    "require_description": RequireDescriptionRule,
    "require_domain": RequireDomainRule,
    "no_deprecated_upstream": NoDeprecatedUpstreamRule,
    "upstream_must_have_owner": UpstreamMustHaveOwnerRule,
    "require_tags": RequireTagsRule,
    "require_column_descriptions": RequireColumnDescriptionsRule,
    "naming_convention": NamingConventionRule,
    "require_materialization": RequireMaterializationRule,
    "max_upstream_dependencies": MaxUpstreamDependenciesRule,
    "require_pii_tag": RequirePIITagRule,
}


class GovernanceEngine:
    """Engine for running governance validation against dbt models."""

    def __init__(
        self,
        config: GovernanceConfig,
        datahub_client: DataHubClient,
        manifest: DbtManifest,
    ) -> None:
        self.config = config
        self.datahub_client = datahub_client
        self.manifest = manifest
        self.urn_mapper = UrnMapper(
            platform=config.target_platform,
            env=config.environment,
            platform_instance=config.platform_instance,
        )
        self._rules: list[BaseRule] = []
        self._initialize_rules()

    def _initialize_rules(self) -> None:
        for rule_name, rule_config in self.config.rules.items():
            if rule_config.enabled and rule_name in RULE_REGISTRY:
                rule_class = RULE_REGISTRY[rule_name]
                self._rules.append(rule_class(rule_config))
                logger.debug(f"Enabled rule: {rule_name}")

    def _should_include_model(self, model: DbtModel) -> bool:
        """Check if a model should be included based on patterns."""
        for pattern in self.config.exclude_patterns:
            if fnmatch.fnmatch(model.name, pattern) or fnmatch.fnmatch(model.path, pattern):
                logger.debug(f"Model {model.name} excluded by pattern: {pattern}")
                return False

        for pattern in self.config.include_patterns:
            if fnmatch.fnmatch(model.name, pattern) or fnmatch.fnmatch(model.path, pattern):
                return True

        return False

    def _fetch_all_governance_statuses(
        self, models: list[DbtModel]
    ) -> dict[str, DatasetGovernanceStatus]:
        """Fetch governance statuses for all models and their dependencies."""
        urns_to_fetch: set[str] = set()

        for model in models:
            urns_to_fetch.add(self.urn_mapper.model_to_urn(model))
            for dep_id in model.depends_on:
                dep_model = self.manifest.get_model(dep_id)
                if dep_model:
                    urns_to_fetch.add(self.urn_mapper.model_to_urn(dep_model))

        logger.info(f"Fetching governance status for {len(urns_to_fetch)} datasets")
        return self.datahub_client.get_governance_status_batch(list(urns_to_fetch))

    def validate(self) -> ValidationReport:
        """Run all governance validations."""
        report = ValidationReport()

        models_to_validate = [
            model
            for model in self.manifest.models.values()
            if self._should_include_model(model)
        ]

        report.total_models_checked = len(models_to_validate)
        logger.info(f"Validating {len(models_to_validate)} models against {len(self._rules)} rules")

        all_statuses = self._fetch_all_governance_statuses(models_to_validate)

        for model in models_to_validate:
            urn = self.urn_mapper.model_to_urn(model)
            status = all_statuses.get(urn, DatasetGovernanceStatus(urn=urn, exists=False))

            for rule in self._rules:
                try:
                    result = rule.validate(model, status, self.manifest, all_statuses)
                    report.add_result(result)
                except Exception as e:
                    logger.error(f"Error running rule {rule.rule_name} on {model.name}: {e}")
                    report.add_result(
                        ValidationResult(
                            rule_name=rule.rule_name,
                            model_name=model.name,
                            model_unique_id=model.unique_id,
                            passed=False,
                            severity=ValidationSeverity.ERROR,
                            message=f"Rule execution error: {e}",
                            details={"error": str(e)},
                        )
                    )

        return report

    def validate_single_model(self, model_name: str) -> ValidationReport:
        """Validate a single model by name."""
        model = self.manifest.get_model_by_name(model_name)
        if not model:
            report = ValidationReport()
            report.add_result(
                ValidationResult(
                    rule_name="model_lookup",
                    model_name=model_name,
                    model_unique_id="",
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Model not found in manifest: {model_name}",
                )
            )
            return report

        report = ValidationReport()
        report.total_models_checked = 1

        urn = self.urn_mapper.model_to_urn(model)
        status = self.datahub_client.get_governance_status(urn)

        all_statuses = {urn: status}
        for dep_id in model.depends_on:
            dep_model = self.manifest.get_model(dep_id)
            if dep_model:
                dep_urn = self.urn_mapper.model_to_urn(dep_model)
                all_statuses[dep_urn] = self.datahub_client.get_governance_status(dep_urn)

        for rule in self._rules:
            try:
                result = rule.validate(model, status, self.manifest, all_statuses)
                report.add_result(result)
            except Exception as e:
                logger.error(f"Error running rule {rule.rule_name} on {model.name}: {e}")
                report.add_result(
                    ValidationResult(
                        rule_name=rule.rule_name,
                        model_name=model.name,
                        model_unique_id=model.unique_id,
                        passed=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Rule execution error: {e}",
                        details={"error": str(e)},
                    )
                )

        return report
