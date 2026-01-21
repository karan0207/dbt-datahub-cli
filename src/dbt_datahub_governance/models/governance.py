"""Data models for governance rules and validation."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ValidationSeverity(str, Enum):
    """Severity level of a validation result."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class GovernanceRule:
    """Configuration for a governance rule."""

    name: str
    enabled: bool = True
    severity: ValidationSeverity = ValidationSeverity.ERROR
    description: Optional[str] = None
    config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.severity, str):
            self.severity = ValidationSeverity(self.severity.lower())


@dataclass
class GovernanceConfig:
    """Configuration for governance validation."""

    rules: dict[str, GovernanceRule]
    target_platform: str = "snowflake"
    environment: str = "PROD"
    platform_instance: Optional[str] = None
    fail_on_warnings: bool = False
    include_patterns: list[str] = field(default_factory=lambda: ["*"])
    exclude_patterns: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GovernanceConfig":
        rules = {}
        for rule_name, rule_config in data.get("rules", {}).items():
            if isinstance(rule_config, bool):
                rules[rule_name] = GovernanceRule(name=rule_name, enabled=rule_config)
            elif isinstance(rule_config, dict):
                rules[rule_name] = GovernanceRule(
                    name=rule_name,
                    enabled=rule_config.get("enabled", True),
                    severity=rule_config.get("severity", "error"),
                    description=rule_config.get("description"),
                    config=rule_config.get("config", {}),
                )
            else:
                rules[rule_name] = GovernanceRule(name=rule_name, enabled=True)

        return cls(
            rules=rules,
            target_platform=data.get("target_platform", "snowflake"),
            environment=data.get("environment", "PROD"),
            platform_instance=data.get("platform_instance"),
            fail_on_warnings=data.get("fail_on_warnings", False),
            include_patterns=data.get("include_patterns", ["*"]),
            exclude_patterns=data.get("exclude_patterns", []),
        )

    @classmethod
    def default(cls) -> "GovernanceConfig":
        return cls(
            rules={
                "require_owner": GovernanceRule(
                    name="require_owner",
                    enabled=True,
                    severity=ValidationSeverity.ERROR,
                    description="All models must have an owner assigned in DataHub",
                ),
                "require_description": GovernanceRule(
                    name="require_description",
                    enabled=True,
                    severity=ValidationSeverity.ERROR,
                    description="All models must have a description",
                ),
                "require_domain": GovernanceRule(
                    name="require_domain",
                    enabled=False,
                    severity=ValidationSeverity.WARNING,
                    description="All models should be assigned to a domain",
                ),
                "no_deprecated_upstream": GovernanceRule(
                    name="no_deprecated_upstream",
                    enabled=True,
                    severity=ValidationSeverity.ERROR,
                    description="Models cannot depend on deprecated upstream datasets",
                ),
                "upstream_must_have_owner": GovernanceRule(
                    name="upstream_must_have_owner",
                    enabled=True,
                    severity=ValidationSeverity.WARNING,
                    description="Upstream dependencies should have owners",
                ),
            }
        )


@dataclass
class DatasetGovernanceStatus:
    """Governance status for a dataset in DataHub."""

    urn: str
    exists: bool
    has_owner: bool = False
    has_description: bool = False
    has_domain: bool = False
    has_tags: bool = False
    is_deprecated: bool = False
    owners: list[str] = field(default_factory=list)
    domain: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    description: Optional[str] = None
    deprecation_note: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of a single validation check."""

    rule_name: str
    model_name: str
    model_unique_id: str
    passed: bool
    severity: ValidationSeverity
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_error(self) -> bool:
        return not self.passed and self.severity == ValidationSeverity.ERROR

    @property
    def is_warning(self) -> bool:
        return not self.passed and self.severity == ValidationSeverity.WARNING


@dataclass
class ValidationReport:
    """Complete validation report for all checked models."""

    results: list[ValidationResult] = field(default_factory=list)
    total_models_checked: int = 0
    total_checks: int = 0
    errors: int = 0
    warnings: int = 0
    passed: int = 0

    def add_result(self, result: ValidationResult) -> None:
        self.results.append(result)
        self.total_checks += 1
        if result.passed:
            self.passed += 1
        elif result.severity == ValidationSeverity.ERROR:
            self.errors += 1
        elif result.severity == ValidationSeverity.WARNING:
            self.warnings += 1

    @property
    def has_errors(self) -> bool:
        return self.errors > 0

    @property
    def has_warnings(self) -> bool:
        return self.warnings > 0

    @property
    def is_successful(self) -> bool:
        return not self.has_errors

    def get_errors(self) -> list[ValidationResult]:
        return [r for r in self.results if r.is_error]

    def get_warnings(self) -> list[ValidationResult]:
        return [r for r in self.results if r.is_warning]

    def get_results_for_model(self, model_unique_id: str) -> list[ValidationResult]:
        return [r for r in self.results if r.model_unique_id == model_unique_id]
        return [r for r in self.results if r.model_unique_id == model_unique_id]

    @staticmethod
    def _make_json_safe(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: ValidationReport._make_json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ValidationReport._make_json_safe(item) for item in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        return str(obj)

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": {
                "total_models_checked": self.total_models_checked,
                "total_checks": self.total_checks,
                "passed": self.passed,
                "errors": self.errors,
                "warnings": self.warnings,
                "success": self.is_successful,
            },
            "results": [
                {
                    "rule_name": r.rule_name,
                    "model_name": r.model_name,
                    "model_unique_id": r.model_unique_id,
                    "passed": r.passed,
                    "severity": r.severity.value,
                    "message": r.message,
                    "details": self._make_json_safe(r.details),
                }
                for r in self.results
            ],
        }
