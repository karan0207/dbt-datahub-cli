"""Validation report output formatters."""

from typing import IO, Optional

from dbt_datahub_governance.constants import REPORTER_FORMATS
from dbt_datahub_governance.reporters.base import BaseReporter
from dbt_datahub_governance.reporters.console import ConsoleReporter
from dbt_datahub_governance.reporters.github import GithubActionsReporter
from dbt_datahub_governance.reporters.json_reporter import JsonReporter
from dbt_datahub_governance.reporters.markdown import MarkdownReporter


def get_reporter(
    format: str,
    verbose: bool = False,
    show_passed: bool = False,
    output: Optional[IO[str]] = None,
) -> BaseReporter:
    """Get a reporter instance by format name."""
    if format not in REPORTER_FORMATS:
        available = ", ".join(REPORTER_FORMATS)
        raise ValueError(f"Unknown format: {format}. Available: {available}")

    if format == "console":
        return ConsoleReporter(verbose=verbose, show_passed=show_passed, output=output)
    elif format == "json":
        return JsonReporter(output=output)
    elif format == "markdown":
        return MarkdownReporter(output=output)
    else:
        return GithubActionsReporter(output=output)


__all__ = [
    "BaseReporter",
    "ConsoleReporter",
    "GithubActionsReporter",
    "JsonReporter",
    "MarkdownReporter",
    "REPORTER_FORMATS",
    "get_reporter",
]
