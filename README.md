# dbt-datahub-governance

A CLI tool that enforces data governance by validating dbt models against governance context stored in DataHub. It brings ownership, lineage awareness, and policy enforcement directly into the dbt workflow.

## Overview

**dbt-datahub-governance** introduces governance checks as a first-class step in the dbt workflow. The tool:

- Reads dbt project metadata (models, descriptions, dependencies)
- Resolves corresponding datasets in DataHub
- Fetches governance context such as ownership, domains, tags, and deprecation status
- Validates dbt models against configurable governance rules
- Fails fast with clear, actionable feedback when violations are detected

## Features

- **Built-in Governance Rules** - Ownership, descriptions, domain assignment, deprecation checks, and more
- **Multiple Output Formats** - Console, JSON, Markdown, and GitHub Actions annotations
- **Flexible Configuration** - YAML-based rules with severity levels and pattern matching
- **CI/CD Ready** - Exit codes and JSON output for pipeline integration
- **DataHub Integration** - Real-time validation against DataHub metadata
- **Web Dashboard** - Interactive Streamlit-based interface for running validations

# Install from source
git clone https://github.com/karan0207/dbt-datahub-cli.git
cd dbt-datahub-cli
pip install -e ".[all]"

# Install with development dependencies
pip install -e ".[dev]"

# Install with dashboard support
pip install -e ".[dashboard]"
```

## Quick Start

### 1. Initialize Configuration

```bash
dbt-datahub-governance init
```

This creates a `governance.yml` file with default settings.

### 2. Configure DataHub Connection

Set environment variables for DataHub connection:

```bash
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_GMS_TOKEN="your-access-token"
```

### 3. Run Validation

```bash
# Validate all models in your dbt project
dbt-datahub-governance validate --manifest target/manifest.json

# Validate a specific model
dbt-datahub-governance validate --manifest target/manifest.json --model dim_customers

# Dry run (without DataHub connection)
dbt-datahub-governance validate --manifest target/manifest.json --dry-run
```

## Commands

### validate

Validate dbt models against DataHub governance rules.

```bash
dbt-datahub-governance validate [OPTIONS]

Options:
  -m, --manifest PATH       Path to dbt manifest.json (required)
  -c, --catalog PATH        Path to dbt catalog.json (optional)
  -C, --config PATH         Path to governance config file
  --datahub-server URL      DataHub GMS server URL
  --datahub-token TOKEN     DataHub access token
  -p, --platform TEXT       Target data platform (e.g., snowflake, bigquery)
  -e, --environment TEXT    DataHub environment (e.g., PROD, DEV)
  -M, --model TEXT          Validate a specific model by name
  -f, --format FORMAT       Output format: console, json, markdown, github (default: console)
  -v, --verbose             Enable verbose output
  -q, --quiet               Suppress non-error output
  --show-passed             Show passing checks in output
  --fail-on-warnings        Exit with failure if there are warnings
  --dry-run                 Run without connecting to DataHub
```

### init

Initialize a new governance configuration file.

```bash
dbt-datahub-governance init [OPTIONS]

Options:
  -o, --output PATH   Output path for config file (default: ./governance.yml)
  -f, --force         Overwrite existing file
```

### list-models

List all models in a dbt manifest with their DataHub URNs.

```bash
dbt-datahub-governance list-models [OPTIONS]

Options:
  -m, --manifest PATH     Path to dbt manifest.json (required)
  -p, --platform TEXT     Target data platform
  -e, --environment TEXT  DataHub environment
```

### test-connection

Test connection to DataHub.

```bash
dbt-datahub-governance test-connection [OPTIONS]

Options:
  --datahub-server URL    DataHub GMS server URL (required)
  --datahub-token TOKEN   DataHub access token
```

### list-rules

List all available governance rules.

```bash
dbt-datahub-governance list-rules
```

### dashboard

Launch the web dashboard for interactive validation.

```bash
dbt-datahub-governance dashboard [OPTIONS]

Options:
  -p, --port INT    Port to run the dashboard on (default: 8501)
  -h, --host TEXT   Host to bind the dashboard to (default: localhost)
```

## Configuration

Create a `governance.yml` file to configure validation rules:

```yaml
# Target data platform (snowflake, bigquery, redshift, postgres, databricks)
target_platform: snowflake

# DataHub environment (PROD, DEV, STAGING)
environment: PROD

# Platform instance (optional, for multi-account setups)
# platform_instance: my-account

# Fail validation if there are warnings
fail_on_warnings: false

# Patterns to include/exclude models
include_patterns:
  - "*"

exclude_patterns:
  - "staging_*"
  - "tmp_*"

# Governance rules
rules:
  require_owner:
    enabled: true
    severity: error
    description: "All models must have an owner assigned in DataHub"

  require_description:
    enabled: true
    severity: error
    description: "All models must have a description"

  require_domain:
    enabled: false
    severity: warning
    description: "All models should be assigned to a domain"

  no_deprecated_upstream:
    enabled: true
    severity: error
    description: "Models cannot depend on deprecated datasets"

  upstream_must_have_owner:
    enabled: true
    severity: warning
    description: "Upstream dependencies should have owners"

  require_tags:
    enabled: false
    severity: warning
    description: "All models should have tags"
```

## Available Rules

| Rule | Description | Default |
|------|-------------|---------|
| `require_owner` | Datasets must have an owner in DataHub | Enabled (error) |
| `require_description` | Models must have a description in dbt or DataHub | Enabled (error) |
| `require_domain` | Datasets must be assigned to a domain | Disabled (warning) |
| `no_deprecated_upstream` | Models cannot depend on deprecated datasets | Enabled (error) |
| `upstream_must_have_owner` | Upstream dependencies should have owners | Enabled (warning) |
| `require_tags` | Models should have tags assigned | Disabled (warning) |

## Output Formats

### Console (Default)

Human-readable output with colors:

```
dbt-datahub-governance Validation Report

  Models Checked    4
  Total Checks     12
  Passed            9
  Errors            2
  Warnings          1

Errors:
  [require_owner] stg_customers: Model does not have an owner assigned in DataHub
  [require_description] stg_orders: Model does not have a description

Warnings:
  [require_domain] dim_customers: Model is not assigned to any domain

Validation failed with 2 error(s)
```

### JSON

Machine-readable JSON output for CI systems:

```bash
dbt-datahub-governance validate -m target/manifest.json -f json
```

```json
{
  "summary": {
    "total_models_checked": 4,
    "total_checks": 12,
    "passed": 9,
    "errors": 2,
    "warnings": 1,
    "success": false
  },
  "results": [
    {
      "rule_name": "require_owner",
      "model_name": "stg_customers",
      "passed": false,
      "severity": "error",
      "message": "Model does not have an owner assigned in DataHub"
    }
  ]
}
```

### Markdown

Generates a formatted report suitable for PR comments and documentation.

### GitHub Actions

Outputs annotations that appear directly in GitHub pull request file views.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Validation passed |
| 1 | Validation failed (errors found) |
| 2 | Runtime error (e.g., connection failure) |

## Project Structure

```
dbt-datahub-governance/
├── src/
│   └── dbt_datahub_governance/
│       ├── cli.py           # CLI entry point
│       ├── dashboard.py     # Streamlit web dashboard
│       ├── config/          # Configuration loading
│       ├── datahub/         # DataHub client and URN mapping
│       ├── models/          # Data models
│       ├── parsers/         # dbt manifest parser
│       ├── reporters/       # Output formatters
│       └── rules/           # Governance rule engine
└── tests/                   # Test suite
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on the contribution process.
