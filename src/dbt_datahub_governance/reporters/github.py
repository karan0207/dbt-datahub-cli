"""GitHub Actions reporter for CI integration."""

import sys
from typing import IO, Optional

from dbt_datahub_governance.constants import (
    SEVERITY_ERROR,
    SEVERITY_NOTICE,
    SEVERITY_WARNING,
)
from dbt_datahub_governance.models.governance import (
    ValidationReport,
    ValidationSeverity,
)
from dbt_datahub_governance.reporters.base import BaseReporter


SEVERITY_LEVELS = {
    ValidationSeverity.ERROR: SEVERITY_ERROR,
    ValidationSeverity.WARNING: SEVERITY_WARNING,
    ValidationSeverity.INFO: SEVERITY_NOTICE,
}


class GithubActionsReporter(BaseReporter):
    """GitHub Actions reporter for CI integration."""

    def __init__(self, output: Optional[IO[str]] = None) -> None:
        self.output = output or sys.stdout

    def report(self, validation_report: ValidationReport) -> None:
        for result in validation_report.results:
            if result.passed:
                continue

            level = SEVERITY_LEVELS.get(result.severity, "notice")
            title = f"{result.rule_name}: {result.model_name}"
            message = result.message.replace("\n", "%0A")
            self.output.write(f"::{level} title={title}::{message}\n")

        self.output.write("\n")
        self.output.write(f"Total models checked: {validation_report.total_models_checked}\n")
        self.output.write(f"Checks passed: {validation_report.passed}\n")
        self.output.write(f"Errors: {validation_report.errors}\n")
        self.output.write(f"Warnings: {validation_report.warnings}\n")
