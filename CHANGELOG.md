# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-20

### Added
- Initial release of dbt-datahub-governance CLI
- 11 built-in governance rules:
  - `require_owner` - Validate models have DataHub owners
  - `require_description` - Validate models have descriptions
  - `require_domain` - Validate models are assigned to domains
  - `no_deprecated_upstream` - Prevent dependencies on deprecated datasets
  - `upstream_must_have_owner` - Validate upstream dependencies have owners
  - `require_tags` - Validate models have tags
  - `require_column_descriptions` - Validate columns have descriptions
  - `naming_convention` - Enforce model naming conventions
  - `require_materialization` - Validate explicit materialization config
  - `max_upstream_dependencies` - Limit upstream dependency count
  - `require_pii_tag` - Require PII tagging for sensitive data
- Multiple output formats: console, JSON, Markdown, GitHub Actions
- YAML-based configuration with `governance.yml`
- DataHub integration via acryl-datahub SDK
- CLI commands: `validate`, `init`, `list-models`, `test-connection`, `list-rules`, `dashboard`
- Docker support
- CI/CD integration with exit codes
- Interactive Streamlit dashboard (optional)

[Unreleased]: https://github.com/AryanSChandel/dbt-governance/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AryanSChandel/dbt-governance/releases/tag/v0.1.0
