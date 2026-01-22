# dbt-datahub-governance

A CLI tool that enforces data governance by validating dbt models against policies stored in DataHub. Brings ownership, lineage awareness, and policy enforcement directly into the dbt workflow.

## Features

- Built-in governance rules: ownership, descriptions, domain assignment, deprecation checks
- Multiple output formats: Console, JSON, Markdown, GitHub Actions annotations
- Flexible YAML-based configuration with severity levels
- CI/CD ready with standard exit codes
- Interactive web dashboard for validation and results

![dbt-datahub-governance](https://media2.dev.to/dynamic/image/width=800%2Cheight=%2Cfit=scale-down%2Cgravity=auto%2Cformat=auto/https%3A%2F%2Fdev-to-uploads.s3.amazonaws.com%2Fuploads%2Farticles%2Fjlfv1qjov00dldbjjig5.png)

## Installation

```bash
git clone https://github.com/karan0207/dbt-datahub-cli.git
cd dbt-datahub-cli
pip install -e ".[all]"
```

**Optional:**
- Development: `pip install -e ".[dev]"`
- Dashboard: `pip install -e ".[dashboard]"`

**Requirements:** Python 3.9+, DataHub instance, dbt project with manifest.json

## Quick Start

1. Initialize configuration:
   ```bash
   dbt-datahub-governance init
   ```

2. Configure DataHub connection:
   ```bash
   export DATAHUB_GMS_URL="http://localhost:8080"
   export DATAHUB_GMS_TOKEN="your-access-token"
   ```

3. Run validation:
   ```bash
   dbt-datahub-governance validate --manifest target/manifest.json
   ```

## Commands

| Command | Purpose |
|---------|---------|
| `validate` | Validate dbt models against governance rules |
| `init` | Initialize governance configuration file |
| `list-models` | List models with DataHub URNs |
| `test-connection` | Test DataHub connectivity |
| `list-rules` | Display available governance rules |
| `dashboard` | Launch web dashboard |

Run `dbt-datahub-governance <command> --help` for detailed options.

## Configuration

Create `governance.yml`:

```yaml
target_platform: snowflake
environment: PROD
fail_on_warnings: false

rules:
  require_owner:
    enabled: true
    severity: error
  require_description:
    enabled: true
    severity: error
  require_domain:
    enabled: false
    severity: warning
  no_deprecated_upstream:
    enabled: true
    severity: error
  upstream_must_have_owner:
    enabled: true
    severity: warning
```

## Available Rules

| Rule | Description | Default |
|------|-------------|---------|
| `require_owner` | Datasets must have an owner | Enabled |
| `require_description` | Models must have a description | Enabled |
| `require_domain` | Datasets must be assigned to a domain | Disabled |
| `no_deprecated_upstream` | Cannot depend on deprecated datasets | Enabled |
| `upstream_must_have_owner` | Upstream dependencies should have owners | Enabled |
| `require_tags` | Models should have tags | Disabled |

## Output Formats

- **Console:** Human-readable colored output (default)
- **JSON:** Machine-readable format for CI systems
- **Markdown:** Formatted reports for pull requests
- **GitHub:** Annotations in GitHub PR views

Usage: `dbt-datahub-governance validate -m target/manifest.json -f <format>`

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Validation passed |
| 1 | Validation failed (errors found) |
| 2 | Runtime error (e.g., connection failure) |

## License

MIT License 

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## Support

For issues or questions, open an issue on GitHub.
