"""Base rule class for governance validation."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.models.governance import (
    DatasetGovernanceStatus,
    GovernanceRule,
    ValidationResult,
    ValidationSeverity,
)


class BaseRule(ABC):
    """Base class for governance rules."""

    rule_name: str = "base_rule"
    description: str = "Base governance rule"

    def __init__(self, config: GovernanceRule) -> None:
        self.config = config
        self.enabled = config.enabled
        self.severity = config.severity

    @abstractmethod
    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        """Validate a model against this rule."""
        pass

    def _create_result(
        self,
        model: DbtModel,
        passed: bool,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> ValidationResult:
        return ValidationResult(
            rule_name=self.rule_name,
            model_name=model.name,
            model_unique_id=model.unique_id,
            passed=passed,
            severity=self.severity,
            message=message,
            details=details or {},
        )
