"""CLI interface for dbt-datahub-governance."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click

from dbt_datahub_governance import __version__
from dbt_datahub_governance.config import load_config, get_datahub_connection_from_env
from dbt_datahub_governance.config.loader import create_default_config_file
from dbt_datahub_governance.constants import (
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
    EXIT_RUNTIME_ERROR,
    DEFAULT_LOG_FORMAT_VERBOSE,
    DEFAULT_LOG_FORMAT_SIMPLE,
)
from dbt_datahub_governance.datahub import DataHubClient, MockDataHubClient
from dbt_datahub_governance.exceptions import (
    ConfigLoadError,
    DataHubConnectionError,
    DbtParserError,
)
from dbt_datahub_governance.models.governance import DatasetGovernanceStatus
from dbt_datahub_governance.parsers import DbtManifestParser, load_dbt_project
from dbt_datahub_governance.reporters import get_reporter
from dbt_datahub_governance.rules import GovernanceEngine


def setup_logging(verbose: bool, quiet: bool) -> None:
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    log_format = DEFAULT_LOG_FORMAT_VERBOSE if verbose else DEFAULT_LOG_FORMAT_SIMPLE
    logging.basicConfig(
        level=level,
        format=log_format,
    )


@click.group()
@click.version_option(version=__version__, prog_name="dbt-datahub-governance")
def main() -> None:
    """Enforce data governance on dbt models using DataHub."""
    pass


@main.command()
@click.option(
    "--manifest",
    "-m",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to dbt manifest.json file.",
)
@click.option(
    "--catalog",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to dbt catalog.json file (optional).",
)
@click.option(
    "--config",
    "-C",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to governance configuration file.",
)
@click.option(
    "--datahub-server",
    envvar="DATAHUB_GMS_URL",
    default=None,
    help="DataHub GMS server URL.",
)
@click.option(
    "--datahub-token",
    envvar="DATAHUB_GMS_TOKEN",
    default=None,
    help="DataHub access token.",
)
@click.option(
    "--platform",
    "-p",
    default=None,
    help="Target data platform (overrides config).",
)
@click.option(
    "--environment",
    "-e",
    default=None,
    help="DataHub environment (overrides config).",
)
@click.option(
    "--model",
    "-M",
    default=None,
    help="Validate a specific model by name.",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["console", "json", "markdown", "github"]),
    default="console",
    help="Output format.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress non-error output.",
)
@click.option(
    "--show-passed",
    is_flag=True,
    help="Show passing checks in output.",
)
@click.option(
    "--fail-on-warnings",
    is_flag=True,
    help="Exit with failure if there are warnings.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Run without connecting to DataHub (for testing).",
)
def validate(
    manifest: Path,
    catalog: Optional[Path],
    config: Optional[Path],
    datahub_server: Optional[str],
    datahub_token: Optional[str],
    platform: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    format: str,
    verbose: bool,
    quiet: bool,
    show_passed: bool,
    fail_on_warnings: bool,
    dry_run: bool,
) -> None:
    """Validate dbt models against DataHub governance rules."""
    setup_logging(verbose, quiet)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        governance_config = load_config(config)

        # Apply CLI overrides
        if platform:
            governance_config.target_platform = platform
        if environment:
            governance_config.environment = environment
        if fail_on_warnings:
            governance_config.fail_on_warnings = True

        # Parse dbt manifest
        logger.info(f"Parsing dbt manifest: {manifest}")
        dbt_manifest = load_dbt_project(manifest, catalog)
        logger.info(f"Found {dbt_manifest.model_count} models")

        # Initialize DataHub client
        if dry_run:
            logger.info("Dry run mode - using mock DataHub client")
            # Create mock data for all models
            from dbt_datahub_governance.datahub.urn_mapper import UrnMapper

            mapper = UrnMapper(
                platform=governance_config.target_platform,
                env=governance_config.environment,
                platform_instance=governance_config.platform_instance,
            )

            mock_data = {}
            for dbt_model in dbt_manifest.models.values():
                urn = mapper.model_to_urn(dbt_model)
                mock_data[urn] = DatasetGovernanceStatus(
                    urn=urn,
                    exists=True,
                    has_owner=True,
                    has_description=bool(dbt_model.description),
                    has_domain=False,
                    has_tags=bool(dbt_model.tags),
                    owners=["urn:li:corpuser:dry-run-owner"],
                    description=dbt_model.description,
                    tags=[f"urn:li:tag:{t}" for t in dbt_model.tags],
                )

            datahub_client: DataHubClient = MockDataHubClient(mock_data=mock_data)
        else:
            # Get DataHub connection
            server = datahub_server
            token = datahub_token

            if not server:
                try:
                    conn = get_datahub_connection_from_env()
                    server = conn["server"]
                    token = token or conn.get("token")
                except ConfigLoadError as e:
                    raise click.ClickException(str(e))

            logger.info(f"Connecting to DataHub: {server}")
            datahub_client = DataHubClient(server=server, token=token)

            try:
                datahub_client.test_connection()
            except DataHubConnectionError as e:
                raise click.ClickException(f"Failed to connect to DataHub: {e}")

        # Create governance engine
        engine = GovernanceEngine(
            config=governance_config,
            datahub_client=datahub_client,
            manifest=dbt_manifest,
        )

        # Run validation
        if model:
            logger.info(f"Validating model: {model}")
            report = engine.validate_single_model(model)
        else:
            logger.info("Running governance validation...")
            report = engine.validate()

        # Output report
        reporter = get_reporter(
            format=format,
            verbose=verbose,
            show_passed=show_passed,
        )
        reporter.report(report)

        # Determine exit code
        if report.has_errors:
            sys.exit(EXIT_VALIDATION_FAILED)
        elif governance_config.fail_on_warnings and report.has_warnings:
            sys.exit(EXIT_VALIDATION_FAILED)
        else:
            sys.exit(EXIT_SUCCESS)

    except DbtParserError as e:
        logger.error(f"Error parsing dbt artifacts: {e}")
        sys.exit(EXIT_RUNTIME_ERROR)
    except ConfigLoadError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(EXIT_RUNTIME_ERROR)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(EXIT_RUNTIME_ERROR)


@main.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output path for configuration file.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing configuration file.",
)
def init(output: Optional[Path], force: bool) -> None:
    """Initialize a new governance configuration file."""
    output_path = output or Path.cwd() / "governance.yml"

    if output_path.exists() and not force:
        raise click.ClickException(
            f"Configuration file already exists: {output_path}. Use --force to overwrite."
        )

    try:
        created_path = create_default_config_file(output_path)
        click.echo(f"Created governance configuration file: {created_path}")
    except Exception as e:
        raise click.ClickException(f"Failed to create configuration file: {e}")


@main.command()
@click.option(
    "--manifest",
    "-m",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to dbt manifest.json file.",
)
@click.option(
    "--platform",
    "-p",
    default="snowflake",
    help="Target data platform.",
)
@click.option(
    "--environment",
    "-e",
    default="PROD",
    help="DataHub environment.",
)
def list_models(manifest: Path, platform: str, environment: str) -> None:
    """List all models in a dbt manifest with their DataHub URNs."""
    try:
        dbt_manifest = load_dbt_project(manifest)

        from dbt_datahub_governance.datahub.urn_mapper import UrnMapper

        mapper = UrnMapper(platform=platform, env=environment)

        click.echo(f"\nFound {dbt_manifest.model_count} models:\n")

        for dbt_model in dbt_manifest.models.values():
            urn = mapper.model_to_urn(dbt_model)
            desc_preview = (dbt_model.description or "")[:50] + "..." if dbt_model.description else "(no description)"

            click.echo(f"  {dbt_model.name}")
            click.echo(f"    Path: {dbt_model.original_file_path}")
            click.echo(f"    URN:  {urn}")
            click.echo(f"    Desc: {desc_preview}")
            click.echo()

    except DbtParserError as e:
        raise click.ClickException(f"Error parsing manifest: {e}")


@main.command()
@click.option(
    "--datahub-server",
    envvar="DATAHUB_GMS_URL",
    required=True,
    help="DataHub GMS server URL.",
)
@click.option(
    "--datahub-token",
    envvar="DATAHUB_GMS_TOKEN",
    default=None,
    help="DataHub access token.",
)
def test_connection(datahub_server: str, datahub_token: Optional[str]) -> None:
    """Test connection to DataHub."""
    click.echo(f"Testing connection to DataHub: {datahub_server}")

    try:
        client = DataHubClient(server=datahub_server, token=datahub_token)
        client.test_connection()
        click.echo("✓ Successfully connected to DataHub")
    except DataHubConnectionError as e:
        raise click.ClickException(f"Failed to connect: {e}")


@main.command("list-rules")
def list_rules() -> None:
    """List all available governance rules."""
    from dbt_datahub_governance.rules import RULE_REGISTRY

    click.echo("\n📋 Available Governance Rules:\n")
    click.echo("-" * 70)

    for rule_name, rule_class in RULE_REGISTRY.items():
        click.echo(f"\n  {click.style(rule_name, fg='cyan', bold=True)}")
        click.echo(f"    {rule_class.description}")

    click.echo("\n" + "-" * 70)
    click.echo(f"\nTotal: {len(RULE_REGISTRY)} rules available")
    click.echo("\nConfigure rules in your governance.yml file:")
    click.echo(click.style("  rules:", fg="yellow"))
    click.echo(click.style("    require_owner:", fg="yellow"))
    click.echo(click.style("      enabled: true", fg="yellow"))
    click.echo(click.style("      severity: error", fg="yellow"))
    click.echo()


@main.command("dashboard")
@click.option(
    "--port",
    "-p",
    default=8501,
    help="Port to run the dashboard on.",
)
@click.option(
    "--host",
    "-h",
    default="localhost",
    help="Host to bind the dashboard to.",
)
def dashboard(port: int, host: str) -> None:
    """Launch the web dashboard."""
    try:
        import streamlit.web.cli as stcli
    except ImportError:
        raise click.ClickException(
            "Streamlit is not installed. Install it with:\n"
            "  pip install 'dbt-datahub-governance[dashboard]'"
        )

    import sys
    from pathlib import Path

    dashboard_path = Path(__file__).parent / "dashboard.py"

    click.echo(f"🚀 Starting dashboard at http://{host}:{port}")
    click.echo("   Press Ctrl+C to stop\n")

    sys.argv = [
        "streamlit",
        "run",
        str(dashboard_path),
        "--server.port", str(port),
        "--server.address", host,
        "--browser.gatherUsageStats", "false",
    ]
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()
