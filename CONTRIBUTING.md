# Contributing to dbt-datahub-cli

Thank you for your interest in contributing. This document provides guidelines for the contribution process.

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Git
- A DataHub instance (optional, for integration testing)

### Setup Development Environment

1. Clone the repository:
   ```bash
   git clone https://github.com/karan0207/dbt-datahub-cli.git
   cd dbt-datahub-cli
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Install pre-commit hooks:
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dbt_datahub_governance --cov-report=term-missing

# Run specific test file
pytest tests/test_rules.py

# Run with verbose output
pytest -v
```

## Adding a New Governance Rule

1. Create the rule class in `src/dbt_datahub_governance/rules/builtin.py`:

   ```python
   class MyNewRule(BaseRule):
       """Rule description."""

       rule_name = "my_new_rule"
       description = "What this rule checks"

       def validate(
           self,
           model: DbtModel,
           status: DatasetGovernanceStatus,
           manifest: DbtManifest,
           all_statuses: dict[str, DatasetGovernanceStatus],
       ) -> ValidationResult:
           if condition_passes:
               return self._create_result(
                   model,
                   passed=True,
                   message="Success message",
               )
           return self._create_result(
               model,
               passed=False,
               message="Failure message",
           )
   ```

2. Register the rule in `RULE_REGISTRY`:

   ```python
   RULE_REGISTRY: dict[str, type[BaseRule]] = {
       # ... existing rules ...
       "my_new_rule": MyNewRule,
   }
   ```

3. Add tests in `tests/test_rules.py`:

   ```python
   class TestMyNewRule:
       def test_passes_when_condition_met(self, sample_dbt_model):
           pass

       def test_fails_when_condition_not_met(self, sample_dbt_model):
           pass
   ```

4. Update the README.md with the new rule documentation

## Pull Request Process

1. Create a feature branch:
   ```bash
   git checkout -b feature/my-feature
   ```

2. Make your changes and ensure tests pass

3. Commit with clear messages (see Commit Guidelines below)

4. Push and create a pull request:
   ```bash
   git push origin feature/my-feature
   ```

5. PR Checklist:
   - Tests added/updated and passing
   - Documentation updated
   - Code formatted with Ruff
   - No breaking changes or clearly documented

## Commit Message Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test changes
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

Example: `git commit -m "feat: add new governance rule for X"`

## Code Style

- Format code with Black: `black src/ tests/`
- Lint with Ruff: `ruff check src/ tests/`
- Type hints are required
- Follow PEP 8 conventions

## Reporting Issues

When reporting issues, include:

1. Python version
2. DataHub version (if applicable)
3. Steps to reproduce
4. Expected vs actual behavior
5. Error messages and logs

## Feature Requests

Feature requests are welcome. Please open an issue describing:

1. The problem you're trying to solve
2. Your proposed solution
3. Any alternatives considered

## License

By contributing, you agree your contributions will be licensed under the MIT License.
