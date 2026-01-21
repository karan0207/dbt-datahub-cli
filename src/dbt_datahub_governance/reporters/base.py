"""Base reporter class."""

from abc import ABC, abstractmethod

from dbt_datahub_governance.models.governance import ValidationReport


class BaseReporter(ABC):
    """Base class for validation reporters."""

    @abstractmethod
    def report(self, validation_report: ValidationReport) -> None:
        """Output the validation report."""
        pass
