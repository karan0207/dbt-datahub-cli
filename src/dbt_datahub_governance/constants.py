"""Constants for dbt-datahub-governance."""

# Exit codes
EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILED = 1
EXIT_RUNTIME_ERROR = 2

# Configuration defaults
DEFAULT_CONFIG_FILENAME = "governance.yml"
ALTERNATE_CONFIG_FILENAMES = ["governance.yaml", ".governance.yml", ".governance.yaml"]

# Logging
DEFAULT_LOG_FORMAT_VERBOSE = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_FORMAT_SIMPLE = "%(message)s"

# Reporter formats
REPORTER_FORMATS = ["console", "json", "markdown", "github"]

# DataHub
DEFAULT_DATAHUB_TIMEOUT = 30

# Validation
DEFAULT_ENVIRONMENT = "PROD"
DEFAULT_TARGET_PLATFORM = "snowflake"

# Severity levels - for GitHub Actions and other reporters
SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_NOTICE = "notice"
