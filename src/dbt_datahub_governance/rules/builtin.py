"""Built-in governance rules."""

from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.models.governance import (
    DatasetGovernanceStatus,
    ValidationResult,
)
from dbt_datahub_governance.rules.base import BaseRule


class RequireOwnerRule(BaseRule):
    """Requires models to have an owner in DataHub."""

    rule_name = "require_owner"
    description = "All models must have an owner assigned in DataHub"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        if not status.exists:
            return self._create_result(
                model,
                passed=False,
                message=f"Dataset not found in DataHub: {status.urn}",
                details={"urn": status.urn, "reason": "not_found"},
            )

        if status.has_owner:
            return self._create_result(
                model,
                passed=True,
                message=f"Model has owner(s): {', '.join(status.owners)}",
                details={"owners": status.owners},
            )

        return self._create_result(
            model,
            passed=False,
            message="Model does not have an owner assigned in DataHub",
            details={"urn": status.urn},
        )


class RequireDescriptionRule(BaseRule):
    """Requires models to have a description."""

    rule_name = "require_description"
    description = "All models must have a description"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        if model.description and model.description.strip():
            return self._create_result(
                model,
                passed=True,
                message="Model has description in dbt manifest",
                details={"source": "dbt", "description": model.description[:100]},
            )

        if status.exists and status.has_description:
            return self._create_result(
                model,
                passed=True,
                message="Model has description in DataHub",
                details={"source": "datahub", "description": (status.description or "")[:100]},
            )

        return self._create_result(
            model,
            passed=False,
            message="Model does not have a description in dbt or DataHub",
            details={"urn": status.urn if status.exists else None},
        )


class RequireDomainRule(BaseRule):
    """Requires models to be assigned to a domain."""

    rule_name = "require_domain"
    description = "All models should be assigned to a domain in DataHub"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        if not status.exists:
            return self._create_result(
                model,
                passed=False,
                message=f"Dataset not found in DataHub: {status.urn}",
                details={"urn": status.urn, "reason": "not_found"},
            )

        if status.has_domain:
            return self._create_result(
                model,
                passed=True,
                message=f"Model is assigned to domain: {status.domain}",
                details={"domain": status.domain},
            )

        return self._create_result(
            model,
            passed=False,
            message="Model is not assigned to any domain in DataHub",
            details={"urn": status.urn},
        )


class NoDeprecatedUpstreamRule(BaseRule):
    """Prevents models from depending on deprecated datasets."""

    rule_name = "no_deprecated_upstream"
    description = "Models cannot depend on deprecated upstream datasets"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        deprecated_upstreams = []

        for dep_id in model.depends_on:
            dep_model = manifest.get_model(dep_id)
            if dep_model:
                for urn, dep_status in all_statuses.items():
                    if dep_status.exists and dep_status.is_deprecated:
                        if dep_model.name.upper() in urn.upper() or dep_model.name.lower() in urn.lower():
                            deprecated_upstreams.append({
                                "name": dep_model.name,
                                "urn": urn,
                                "note": dep_status.deprecation_note,
                            })

        if deprecated_upstreams:
            dep_names = [d["name"] for d in deprecated_upstreams]
            return self._create_result(
                model,
                passed=False,
                message=f"Model depends on deprecated datasets: {', '.join(dep_names)}",
                details={"deprecated_upstreams": deprecated_upstreams},
            )

        return self._create_result(
            model,
            passed=True,
            message="No deprecated upstream dependencies found",
            details={"checked_dependencies": len(model.depends_on)},
        )


class UpstreamMustHaveOwnerRule(BaseRule):
    """Requires upstream dependencies to have owners."""

    rule_name = "upstream_must_have_owner"
    description = "Upstream dependencies should have owners"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        unowned_upstreams = []

        for dep_id in model.depends_on:
            dep_model = manifest.get_model(dep_id)
            if dep_model:
                for urn, dep_status in all_statuses.items():
                    if dep_status.exists and not dep_status.has_owner:
                        if dep_model.name.upper() in urn.upper() or dep_model.name.lower() in urn.lower():
                            unowned_upstreams.append({"name": dep_model.name, "urn": urn})

        if unowned_upstreams:
            dep_names = [d["name"] for d in unowned_upstreams]
            return self._create_result(
                model,
                passed=False,
                message=f"Upstream dependencies without owners: {', '.join(dep_names)}",
                details={"unowned_upstreams": unowned_upstreams},
            )

        return self._create_result(
            model,
            passed=True,
            message="All upstream dependencies have owners",
            details={"checked_dependencies": len(model.depends_on)},
        )


class RequireTagsRule(BaseRule):
    """Requires models to have tags."""

    rule_name = "require_tags"
    description = "All models should have tags assigned"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        if model.tags:
            return self._create_result(
                model,
                passed=True,
                message=f"Model has dbt tags: {', '.join(model.tags)}",
                details={"source": "dbt", "tags": model.tags},
            )

        if status.exists and status.has_tags:
            return self._create_result(
                model,
                passed=True,
                message=f"Model has DataHub tags: {', '.join(status.tags)}",
                details={"source": "datahub", "tags": status.tags},
            )

        return self._create_result(
            model,
            passed=False,
            message="Model does not have any tags in dbt or DataHub",
            details={"urn": status.urn if status.exists else None},
        )


class RequireColumnDescriptionsRule(BaseRule):
    """Requires all columns to have descriptions."""

    rule_name = "require_column_descriptions"
    description = "All columns should have descriptions"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        if not model.columns:
            return self._create_result(
                model,
                passed=True,
                message="No columns defined in model",
                details={"column_count": 0},
            )

        missing = [
            col_name
            for col_name, col_info in model.columns.items()
            if not col_info.get("description", "").strip()
        ]

        if missing:
            preview = ", ".join(missing[:5]) + ("..." if len(missing) > 5 else "")
            return self._create_result(
                model,
                passed=False,
                message=f"Columns missing descriptions: {preview}",
                details={
                    "missing_columns": missing,
                    "total_columns": len(model.columns),
                    "missing_count": len(missing),
                },
            )

        return self._create_result(
            model,
            passed=True,
            message=f"All {len(model.columns)} columns have descriptions",
            details={"column_count": len(model.columns)},
        )


class NamingConventionRule(BaseRule):
    """Enforces naming conventions for models."""

    rule_name = "naming_convention"
    description = "Models should follow naming conventions (stg_, int_, dim_, fct_)"

    VALID_PREFIXES = ["stg_", "int_", "dim_", "fct_", "rpt_", "base_", "raw_"]

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        name = model.name.lower()

        for prefix in self.VALID_PREFIXES:
            if name.startswith(prefix):
                return self._create_result(
                    model,
                    passed=True,
                    message=f"Model follows naming convention with prefix '{prefix}'",
                    details={"prefix": prefix},
                )

        return self._create_result(
            model,
            passed=False,
            message=f"Model name '{model.name}' does not follow naming conventions (expected: {', '.join(self.VALID_PREFIXES)})",
            details={"valid_prefixes": self.VALID_PREFIXES, "model_name": model.name},
        )


class RequireMaterializationRule(BaseRule):
    """Checks for explicit materialization configuration."""

    rule_name = "require_materialization"
    description = "Models should have explicit materialization configured"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        materialized = model.config.get("materialized")

        if materialized:
            return self._create_result(
                model,
                passed=True,
                message=f"Model has materialization: {materialized}",
                details={"materialization": materialized},
            )

        return self._create_result(
            model,
            passed=False,
            message="Model does not have explicit materialization configured",
            details={"config": model.config},
        )


class MaxUpstreamDependenciesRule(BaseRule):
    """Limits the number of upstream dependencies."""

    rule_name = "max_upstream_dependencies"
    description = "Models should not have too many direct upstream dependencies"

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        max_deps = self.config.config.get("max_dependencies", 10)
        dep_count = len(model.depends_on)

        if dep_count > max_deps:
            return self._create_result(
                model,
                passed=False,
                message=f"Model has {dep_count} upstream dependencies (max: {max_deps})",
                details={
                    "dependency_count": dep_count,
                    "max_allowed": max_deps,
                    "dependencies": model.depends_on[:10],
                },
            )

        return self._create_result(
            model,
            passed=True,
            message=f"Model has {dep_count} upstream dependencies (within limit of {max_deps})",
            details={"dependency_count": dep_count, "max_allowed": max_deps},
        )


class RequirePIITagRule(BaseRule):
    """Requires PII-containing models to be tagged."""

    rule_name = "require_pii_tag"
    description = "Models with PII columns should have 'pii' tag"

    PII_INDICATORS = ["email", "phone", "ssn", "address", "name", "dob", "birth"]

    def validate(
        self,
        model: DbtModel,
        status: DatasetGovernanceStatus,
        manifest: DbtManifest,
        all_statuses: dict[str, DatasetGovernanceStatus],
    ) -> ValidationResult:
        pii_columns = []

        for col_name, col_info in model.columns.items():
            col_lower = col_name.lower()
            if col_info.get("meta", {}).get("pii"):
                pii_columns.append(col_name)
            elif any(indicator in col_lower for indicator in self.PII_INDICATORS):
                pii_columns.append(col_name)

        if not pii_columns:
            return self._create_result(
                model,
                passed=True,
                message="No PII columns detected",
                details={"checked_columns": len(model.columns)},
            )

        has_pii_tag = "pii" in [t.lower() for t in model.tags]

        if has_pii_tag:
            return self._create_result(
                model,
                passed=True,
                message="Model with PII columns is properly tagged",
                details={"pii_columns": pii_columns},
            )

        return self._create_result(
            model,
            passed=False,
            message=f"Model has PII columns ({', '.join(pii_columns[:3])}) but is not tagged with 'pii'",
            details={"pii_columns": pii_columns, "current_tags": model.tags},
        )
