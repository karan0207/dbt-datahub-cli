"""Tests for DataHub client and URN mapper."""

import pytest

from dbt_datahub_governance.datahub import (
    DataHubClient,
    MockDataHubClient,
)
from dbt_datahub_governance.datahub.urn_mapper import UrnMapper, parse_urn
from dbt_datahub_governance.models.dbt_models import DbtModel
from dbt_datahub_governance.models.governance import DatasetGovernanceStatus


class TestUrnMapper:
    """Tests for UrnMapper class."""

    def test_build_urn_snowflake(self) -> None:
        """Test building URN for Snowflake platform."""
        mapper = UrnMapper(platform="snowflake", env="PROD")
        urn = mapper.build_urn("MY_DB.MY_SCHEMA.MY_TABLE")

        assert "snowflake" in urn
        assert "MY_DB.MY_SCHEMA.MY_TABLE" in urn
        assert "PROD" in urn

    def test_build_urn_bigquery(self) -> None:
        """Test building URN for BigQuery platform."""
        mapper = UrnMapper(platform="bigquery", env="PROD")
        urn = mapper.build_urn("my-project.my_dataset.my_table")

        assert "bigquery" in urn
        assert "my-project.my_dataset.my_table" in urn

    def test_build_urn_with_platform_instance(self) -> None:
        """Test building URN with platform instance."""
        mapper = UrnMapper(
            platform="snowflake",
            env="PROD",
            platform_instance="my-account",
        )
        urn = mapper.build_urn("MY_DB.MY_SCHEMA.MY_TABLE")

        assert "my-account" in urn

    def test_model_to_urn(self, sample_dbt_model: DbtModel) -> None:
        """Test converting dbt model to URN."""
        mapper = UrnMapper(platform="snowflake", env="PROD")
        urn = mapper.model_to_urn(sample_dbt_model)

        assert "snowflake" in urn
        # Snowflake normalizes to uppercase
        assert "ANALYTICS_DB" in urn or "analytics_db" in urn.lower()
        assert "MARTS" in urn or "marts" in urn.lower()
        assert "DIM_CUSTOMERS" in urn or "dim_customers" in urn.lower()

    def test_normalize_name_snowflake(self) -> None:
        """Test name normalization for Snowflake (uppercase)."""
        mapper = UrnMapper(platform="snowflake")
        normalized = mapper._normalize_name("my_table")
        assert normalized == "MY_TABLE"

    def test_normalize_name_postgres(self) -> None:
        """Test name normalization for Postgres (lowercase)."""
        mapper = UrnMapper(platform="postgres")
        normalized = mapper._normalize_name("MY_TABLE")
        assert normalized == "my_table"

    def test_normalize_name_other_platform(self) -> None:
        """Test name normalization for other platforms (unchanged)."""
        mapper = UrnMapper(platform="custom_platform")
        normalized = mapper._normalize_name("My_Table")
        assert normalized == "My_Table"

    def test_get_dataset_name_with_database(self, sample_dbt_model: DbtModel) -> None:
        """Test getting dataset name with database."""
        mapper = UrnMapper(platform="snowflake")
        name = mapper.get_dataset_name(sample_dbt_model)

        assert "ANALYTICS_DB" in name
        assert "MARTS" in name
        assert "DIM_CUSTOMERS" in name

    def test_get_dataset_name_custom_overrides(self, sample_dbt_model: DbtModel) -> None:
        """Test getting dataset name with custom overrides."""
        mapper = UrnMapper(
            platform="snowflake",
            custom_database="CUSTOM_DB",
            custom_schema="CUSTOM_SCHEMA",
        )
        name = mapper.get_dataset_name(sample_dbt_model)

        assert "CUSTOM_DB" in name
        assert "CUSTOM_SCHEMA" in name

    def test_source_to_urn(self) -> None:
        """Test converting source to URN."""
        mapper = UrnMapper(platform="snowflake", env="PROD")
        urn = mapper.source_to_urn(
            source_name="customers",
            source_database="RAW_DB",
            source_schema="RAW",
        )

        assert "snowflake" in urn
        assert "RAW_DB" in urn
        assert "RAW" in urn
        assert "CUSTOMERS" in urn


class TestParseUrn:
    """Tests for parse_urn function."""

    def test_parse_valid_urn(self) -> None:
        """Test parsing a valid URN."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.MY_TABLE,PROD)"
        parsed = parse_urn(urn)

        assert parsed["platform"] == "snowflake"
        assert parsed["name"] == "MY_DB.MY_SCHEMA.MY_TABLE"
        assert parsed["env"] == "PROD"

    def test_parse_invalid_urn(self) -> None:
        """Test parsing an invalid URN returns empty dict."""
        urn = "invalid-urn-format"
        parsed = parse_urn(urn)

        assert parsed["platform"] == ""
        assert parsed["name"] == ""
        assert parsed["env"] == ""


class TestMockDataHubClient:
    """Tests for MockDataHubClient class."""

    def test_test_connection(self) -> None:
        """Test that mock client always returns True for connection test."""
        client = MockDataHubClient()
        assert client.test_connection() is True

    def test_dataset_exists_with_mock_data(
        self, sample_governance_status_with_owner: DatasetGovernanceStatus
    ) -> None:
        """Test dataset_exists with mock data."""
        client = MockDataHubClient(
            mock_data={sample_governance_status_with_owner.urn: sample_governance_status_with_owner}
        )

        assert client.dataset_exists(sample_governance_status_with_owner.urn) is True
        assert client.dataset_exists("urn:li:dataset:nonexistent") is False

    def test_get_governance_status(
        self, sample_governance_status_with_owner: DatasetGovernanceStatus
    ) -> None:
        """Test getting governance status from mock data."""
        client = MockDataHubClient(
            mock_data={sample_governance_status_with_owner.urn: sample_governance_status_with_owner}
        )

        status = client.get_governance_status(sample_governance_status_with_owner.urn)

        assert status.exists is True
        assert status.has_owner is True
        assert len(status.owners) > 0

    def test_get_governance_status_not_found(self) -> None:
        """Test getting governance status for non-existent dataset."""
        client = MockDataHubClient()
        status = client.get_governance_status("urn:li:dataset:nonexistent")

        assert status.exists is False

    def test_add_mock_dataset(self) -> None:
        """Test adding mock dataset."""
        client = MockDataHubClient()

        new_status = DatasetGovernanceStatus(
            urn="urn:li:dataset:new",
            exists=True,
            has_owner=True,
            owners=["test-owner"],
        )
        client.add_mock_dataset(new_status)

        assert client.dataset_exists("urn:li:dataset:new") is True
        retrieved = client.get_governance_status("urn:li:dataset:new")
        assert retrieved.has_owner is True


class TestDataHubClientBuildUrn:
    """Tests for DataHubClient.build_dataset_urn method."""

    def test_build_urn_without_sdk(self) -> None:
        """Test building URN without SDK (fallback)."""
        # This tests the manual URN construction fallback
        client = DataHubClient(server="http://localhost:8080")
        urn = client.build_dataset_urn(
            platform="snowflake",
            dataset_name="MY_DB.MY_SCHEMA.MY_TABLE",
            env="PROD",
        )

        assert "snowflake" in urn
        assert "MY_DB.MY_SCHEMA.MY_TABLE" in urn
        assert "PROD" in urn

    def test_build_urn_with_platform_instance(self) -> None:
        """Test building URN with platform instance."""
        client = DataHubClient(server="http://localhost:8080")
        urn = client.build_dataset_urn(
            platform="snowflake",
            dataset_name="MY_DB.MY_SCHEMA.MY_TABLE",
            env="PROD",
            platform_instance="my-account",
        )

        assert "my-account" in urn


class TestDataHubClientGetGovernanceStatusBatch:
    """Tests for batch governance status fetching."""

    def test_get_governance_status_batch(
        self,
        sample_governance_status_with_owner: DatasetGovernanceStatus,
        sample_governance_status_without_owner: DatasetGovernanceStatus,
    ) -> None:
        """Test getting governance status for multiple datasets."""
        client = MockDataHubClient(
            mock_data={
                sample_governance_status_with_owner.urn: sample_governance_status_with_owner,
                sample_governance_status_without_owner.urn: sample_governance_status_without_owner,
            }
        )

        urns = [
            sample_governance_status_with_owner.urn,
            sample_governance_status_without_owner.urn,
            "urn:li:dataset:nonexistent",
        ]

        results = client.get_governance_status_batch(urns)

        assert len(results) == 3
        assert results[sample_governance_status_with_owner.urn].has_owner is True
        assert results[sample_governance_status_without_owner.urn].has_owner is False
        assert results["urn:li:dataset:nonexistent"].exists is False
