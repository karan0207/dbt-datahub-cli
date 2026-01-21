"""Custom exceptions for dbt-datahub-governance."""


class GovernanceError(Exception):
    """Base exception for dbt-datahub-governance."""


class ConfigLoadError(GovernanceError):
    """Error raised when configuration loading fails."""


class DbtParserError(GovernanceError):
    """Error raised when dbt artifact parsing fails."""


class DataHubClientError(GovernanceError):
    """Error raised when DataHub API operations fail."""


class DataHubConnectionError(DataHubClientError):
    """Error raised when connection to DataHub fails."""


class ReporterError(GovernanceError):
    """Error raised when report generation fails."""


class ValidationError(GovernanceError):
    """Error raised during governance validation."""
