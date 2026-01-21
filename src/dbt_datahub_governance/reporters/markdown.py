"""Markdown reporter for documentation and PR comments."""

import sys
from typing import IO, Optional

from dbt_datahub_governance.models.governance import ValidationReport
from dbt_datahub_governance.reporters.base import BaseReporter


class MarkdownReporter(BaseReporter):
    """Markdown reporter for documentation/PR comments."""

    def __init__(self, output: Optional[IO[str]] = None) -> None:
        self.output = output or sys.stdout

    def report(self, validation_report: ValidationReport) -> None:
        lines = []
        status_emoji = "✅" if validation_report.is_successful else "❌"

        lines.append("# dbt-datahub-governance Validation Report")
        lines.append("")
        lines.append(f"## Summary {status_emoji}")
        lines.append("")
        lines.append(f"- **Models Checked:** {validation_report.total_models_checked}")
        lines.append(f"- **Total Checks:** {validation_report.total_checks}")
        lines.append(f"- **Passed:** {validation_report.passed}")
        lines.append(f"- **Errors:** {validation_report.errors}")
        lines.append(f"- **Warnings:** {validation_report.warnings}")
        lines.append("")

        errors = validation_report.get_errors()
        if errors:
            lines.append("## Errors ❌")
            lines.append("")
            lines.append("| Model | Rule | Message |")
            lines.append("|-------|------|---------|")
            for result in errors:
                lines.append(f"| `{result.model_name}` | {result.rule_name} | {result.message} |")
            lines.append("")

        warnings = validation_report.get_warnings()
        if warnings:
            lines.append("## Warnings ⚠️")
            lines.append("")
            lines.append("| Model | Rule | Message |")
            lines.append("|-------|------|---------|")
            for result in warnings:
                lines.append(f"| `{result.model_name}` | {result.rule_name} | {result.message} |")
            lines.append("")

        self.output.write("\n".join(lines))
