"""Console reporter using Rich."""

import sys
from typing import IO, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dbt_datahub_governance.models.governance import (
    ValidationReport,
    ValidationResult,
    ValidationSeverity,
)
from dbt_datahub_governance.reporters.base import BaseReporter


class ConsoleReporter(BaseReporter):
    """Rich console reporter for human-readable output."""

    SEVERITY_STYLES = {
        ValidationSeverity.ERROR: "bold red",
        ValidationSeverity.WARNING: "bold yellow",
        ValidationSeverity.INFO: "bold blue",
    }

    def __init__(
        self,
        verbose: bool = False,
        show_passed: bool = False,
        output: Optional[IO[str]] = None,
    ) -> None:
        self.verbose = verbose
        self.show_passed = show_passed
        self.console = Console(file=output or sys.stdout)

    def _get_status_icon(self, result: ValidationResult) -> str:
        if result.passed:
            return "✓"
        elif result.severity == ValidationSeverity.ERROR:
            return "✗"
        elif result.severity == ValidationSeverity.WARNING:
            return "⚠"
        return "ℹ"

    def _format_result(self, result: ValidationResult) -> str:
        icon = self._get_status_icon(result)
        style = "green" if result.passed else self.SEVERITY_STYLES.get(result.severity, "white")
        return f"  [{style}]{icon}[/{style}] [dim][{result.rule_name}][/dim] [bold]{result.model_name}[/bold]: {result.message}"

    def report(self, validation_report: ValidationReport) -> None:
        self.console.print()
        self.console.print(
            Panel.fit("[bold]dbt-datahub-governance Validation Report[/bold]", border_style="blue")
        )
        self.console.print()

        summary_table = Table(show_header=False, box=None, padding=(0, 2))
        summary_table.add_column("Label", style="dim")
        summary_table.add_column("Value", justify="right")
        summary_table.add_row("Models Checked", str(validation_report.total_models_checked))
        summary_table.add_row("Total Checks", str(validation_report.total_checks))
        summary_table.add_row("Passed", f"[green]{validation_report.passed}[/green]")
        summary_table.add_row("Errors", f"[red]{validation_report.errors}[/red]")
        summary_table.add_row("Warnings", f"[yellow]{validation_report.warnings}[/yellow]")
        self.console.print(summary_table)
        self.console.print()

        errors = validation_report.get_errors()
        if errors:
            self.console.print("[bold red]Errors:[/bold red]")
            for result in errors:
                self.console.print(self._format_result(result))
            self.console.print()

        warnings = validation_report.get_warnings()
        if warnings:
            self.console.print("[bold yellow]Warnings:[/bold yellow]")
            for result in warnings:
                self.console.print(self._format_result(result))
            self.console.print()

        if self.show_passed or self.verbose:
            passed = [r for r in validation_report.results if r.passed]
            if passed:
                self.console.print("[bold green]Passed:[/bold green]")
                for result in passed:
                    self.console.print(self._format_result(result))
                self.console.print()

        if validation_report.is_successful:
            self.console.print(
                Panel.fit("[bold green]✓ Validation passed[/bold green]", border_style="green")
            )
        else:
            self.console.print(
                Panel.fit(
                    f"[bold red]✗ Validation failed with {validation_report.errors} error(s)[/bold red]",
                    border_style="red",
                )
            )
