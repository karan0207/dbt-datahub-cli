"""DataHub client for fetching governance metadata."""

import logging
from typing import Any, Optional

from dbt_datahub_governance.exceptions import (
    DataHubClientError,
    DataHubConnectionError,
)
from dbt_datahub_governance.models.governance import DatasetGovernanceStatus

logger = logging.getLogger(__name__)


class DataHubClient:
    """Client for interacting with DataHub to fetch governance metadata."""

    def __init__(
        self,
        server: str,
        token: Optional[str] = None,
        timeout: int = 30,
        disable_ssl_verification: bool = False,
    ) -> None:
        self.server = server.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.disable_ssl_verification = disable_ssl_verification
        self._graph: Optional[Any] = None

    def _get_graph(self) -> Any:
        """Get or create the DataHubGraph client."""
        if self._graph is None:
            try:
                from datahub.ingestion.graph.client import (
                    DataHubGraph,
                    DatahubClientConfig,
                )

                config = DatahubClientConfig(
                    server=self.server,
                    token=self.token,
                    timeout_sec=self.timeout,
                    disable_ssl_verification=self.disable_ssl_verification,
                )
                self._graph = DataHubGraph(config)
            except ImportError:
                raise DataHubConnectionError(
                    "DataHub SDK not installed. Install with: pip install acryl-datahub"
                )
            except Exception as e:
                raise DataHubConnectionError(f"Failed to create DataHub client: {e}")
        return self._graph

    def test_connection(self) -> bool:
        """Test the connection to DataHub."""
        try:
            graph = self._get_graph()
            graph.test_connection()
            logger.info(f"Successfully connected to DataHub at {self.server}")
            return True
        except Exception as e:
            raise DataHubConnectionError(f"Failed to connect to DataHub: {e}")

    def build_dataset_urn(
        self,
        platform: str,
        dataset_name: str,
        env: str = "PROD",
        platform_instance: Optional[str] = None,
    ) -> str:
        """Build a DataHub dataset URN from components."""
        try:
            from datahub.emitter.mce_builder import (
                make_dataset_urn,
                make_dataset_urn_with_platform_instance,
            )

            if platform_instance:
                return make_dataset_urn_with_platform_instance(
                    platform=platform,
                    name=dataset_name,
                    platform_instance=platform_instance,
                    env=env,
                )
            return make_dataset_urn(platform=platform, name=dataset_name, env=env)
        except ImportError:
            if platform_instance:
                return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{platform_instance}.{dataset_name},{env})"
            return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"

    def dataset_exists(self, dataset_urn: str) -> bool:
        """Check if a dataset exists in DataHub."""
        try:
            graph = self._get_graph()
            return graph.exists(dataset_urn)
        except Exception as e:
            logger.warning(f"Error checking dataset existence: {e}")
            return False

    def get_governance_status(self, dataset_urn: str) -> DatasetGovernanceStatus:
        """Get comprehensive governance status for a dataset."""
        if not self.dataset_exists(dataset_urn):
            return DatasetGovernanceStatus(urn=dataset_urn, exists=False)

        graph = self._get_graph()

        # Get ownership
        owners: list[str] = []
        has_owner = False
        try:
            ownership = graph.get_ownership(dataset_urn)
            if ownership and ownership.owners:
                owners = [str(o.owner) for o in ownership.owners]
                has_owner = len(owners) > 0
        except Exception as e:
            logger.debug(f"Error fetching ownership for {dataset_urn}: {e}")

        # Get description from dataset properties
        description: Optional[str] = None
        has_description = False
        try:
            props = graph.get_dataset_properties(dataset_urn)
            if props and props.description:
                description = props.description
                has_description = bool(description and description.strip())
        except Exception as e:
            logger.debug(f"Error fetching properties for {dataset_urn}: {e}")

        # Get domain
        domain: Optional[str] = None
        has_domain = False
        try:
            from datahub.metadata.schema_classes import DomainsClass

            domains_aspect = graph.get_aspect(entity_urn=dataset_urn, aspect_type=DomainsClass)
            if domains_aspect and domains_aspect.domains:
                domain = domains_aspect.domains[0]
                has_domain = True
        except Exception as e:
            logger.debug(f"Error fetching domain for {dataset_urn}: {e}")

        # Get tags
        tags: list[str] = []
        has_tags = False
        try:
            tags_aspect = graph.get_tags(dataset_urn)
            if tags_aspect and tags_aspect.tags:
                tags = [str(t.tag) for t in tags_aspect.tags]
                has_tags = len(tags) > 0
        except Exception as e:
            logger.debug(f"Error fetching tags for {dataset_urn}: {e}")

        # Get deprecation status
        is_deprecated = False
        deprecation_note: Optional[str] = None
        try:
            from datahub.metadata.schema_classes import DeprecationClass

            deprecation = graph.get_aspect(entity_urn=dataset_urn, aspect_type=DeprecationClass)
            if deprecation and deprecation.deprecated:
                is_deprecated = True
                deprecation_note = deprecation.note
        except Exception as e:
            logger.debug(f"Error fetching deprecation for {dataset_urn}: {e}")

        return DatasetGovernanceStatus(
            urn=dataset_urn,
            exists=True,
            has_owner=has_owner,
            has_description=has_description,
            has_domain=has_domain,
            has_tags=has_tags,
            is_deprecated=is_deprecated,
            owners=owners,
            domain=domain,
            tags=tags,
            description=description,
            deprecation_note=deprecation_note,
        )

    def get_governance_status_batch(
        self, dataset_urns: list[str]
    ) -> dict[str, DatasetGovernanceStatus]:
        """Get governance status for multiple datasets."""
        results = {}
        for urn in dataset_urns:
            try:
                results[urn] = self.get_governance_status(urn)
            except Exception as e:
                logger.warning(f"Error fetching governance status for {urn}: {e}")
                results[urn] = DatasetGovernanceStatus(urn=urn, exists=False)
        return results


class MockDataHubClient(DataHubClient):
    """Mock DataHub client for testing without a real DataHub instance."""

    def __init__(
        self,
        server: str = "http://localhost:8080",
        token: Optional[str] = None,
        mock_data: Optional[dict[str, DatasetGovernanceStatus]] = None,
    ) -> None:
        super().__init__(server, token)
        self._mock_data = mock_data or {}

    def test_connection(self) -> bool:
        return True

    def dataset_exists(self, dataset_urn: str) -> bool:
        return dataset_urn in self._mock_data

    def get_governance_status(self, dataset_urn: str) -> DatasetGovernanceStatus:
        if dataset_urn in self._mock_data:
            return self._mock_data[dataset_urn]
        return DatasetGovernanceStatus(urn=dataset_urn, exists=False)

    def add_mock_dataset(self, status: DatasetGovernanceStatus) -> None:
        self._mock_data[status.urn] = status
