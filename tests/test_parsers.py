"""Tests for dbt manifest parser."""

import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from dbt_datahub_governance.models.dbt_models import DbtManifest, DbtModel
from dbt_datahub_governance.parsers import (
    DbtCatalogParser,
    DbtManifestParser,
    DbtParserError,
    load_dbt_project,
)


class TestDbtModel:
    """Tests for DbtModel dataclass."""

    def test_from_manifest_node(self, sample_manifest_data: dict[str, Any]) -> None:
        """Test creating DbtModel from manifest node."""
        node = sample_manifest_data["nodes"]["model.test_project.dim_customers"]
        model = DbtModel.from_manifest_node(node)

        assert model.unique_id == "model.test_project.dim_customers"
        assert model.name == "dim_customers"
        assert model.database == "ANALYTICS_DB"
        assert model.schema == "MARTS"
        assert model.description == "Customer dimension table"
        assert model.resource_type == "model"
        assert "model.test_project.stg_customers" in model.depends_on
        assert "pii" in model.tags
        assert "core" in model.tags

    def test_full_name_with_database(self) -> None:
        """Test full_name property with database."""
        model = DbtModel(
            unique_id="model.test.my_model",
            name="my_model",
            database="MY_DB",
            schema="MY_SCHEMA",
            description="Test",
            resource_type="model",
            package_name="test",
            path="test.sql",
            original_file_path="models/test.sql",
        )
        assert model.full_name == "MY_DB.MY_SCHEMA.my_model"

    def test_full_name_without_database(self) -> None:
        """Test full_name property without database."""
        model = DbtModel(
            unique_id="model.test.my_model",
            name="my_model",
            database=None,
            schema="MY_SCHEMA",
            description="Test",
            resource_type="model",
            package_name="test",
            path="test.sql",
            original_file_path="models/test.sql",
        )
        assert model.full_name == "MY_SCHEMA.my_model"


class TestDbtManifest:
    """Tests for DbtManifest dataclass."""

    def test_model_count(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test model_count property."""
        assert sample_dbt_manifest.model_count == 2

    def test_get_model(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test get_model method."""
        model = sample_dbt_manifest.get_model("model.test_project.dim_customers")
        assert model is not None
        assert model.name == "dim_customers"

    def test_get_model_not_found(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test get_model returns None for non-existent model."""
        model = sample_dbt_manifest.get_model("model.test_project.nonexistent")
        assert model is None

    def test_get_model_by_name(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test get_model_by_name method."""
        model = sample_dbt_manifest.get_model_by_name("dim_customers")
        assert model is not None
        assert model.unique_id == "model.test_project.dim_customers"

    def test_get_upstream_models(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test get_upstream_models method."""
        dim_customers = sample_dbt_manifest.get_model_by_name("dim_customers")
        assert dim_customers is not None

        upstream = sample_dbt_manifest.get_upstream_models(dim_customers)
        assert len(upstream) == 1
        assert upstream[0].name == "stg_customers"

    def test_get_all_model_names(self, sample_dbt_manifest: DbtManifest) -> None:
        """Test get_all_model_names method."""
        names = sample_dbt_manifest.get_all_model_names()
        assert "dim_customers" in names
        assert "stg_customers" in names


class TestDbtManifestParser:
    """Tests for DbtManifestParser class."""

    def test_parse_valid_manifest(self, sample_manifest_file: Path) -> None:
        """Test parsing a valid manifest file."""
        parser = DbtManifestParser(sample_manifest_file)
        manifest = parser.parse()

        assert manifest.model_count == 4
        assert manifest.dbt_version == "1.7.0"
        assert "model.test_project.dim_customers" in manifest.models

    def test_parse_nonexistent_file(self) -> None:
        """Test parsing a non-existent file raises error."""
        with pytest.raises(DbtParserError, match="Manifest file not found"):
            DbtManifestParser("/nonexistent/path/manifest.json")

    def test_parse_invalid_json(self) -> None:
        """Test parsing invalid JSON raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {")
            temp_path = Path(f.name)

        try:
            parser = DbtManifestParser(temp_path)
            with pytest.raises(DbtParserError, match="Invalid JSON"):
                parser.parse()
        finally:
            temp_path.unlink()

    def test_get_models(self, sample_manifest_file: Path) -> None:
        """Test get_models method."""
        parser = DbtManifestParser(sample_manifest_file)
        models = parser.get_models()

        assert len(models) == 4
        assert all(isinstance(m, DbtModel) for m in models.values())

    def test_get_model_dependencies(
        self, sample_manifest_file: Path
    ) -> None:
        """Test get_model_dependencies method."""
        parser = DbtManifestParser(sample_manifest_file)
        manifest = parser.parse()

        dim_customers = manifest.get_model_by_name("dim_customers")
        assert dim_customers is not None

        deps = parser.get_model_dependencies(dim_customers)
        assert "model.test_project.stg_customers" in deps

    def test_caching(self, sample_manifest_file: Path) -> None:
        """Test that parsing is cached."""
        parser = DbtManifestParser(sample_manifest_file)

        manifest1 = parser.parse()
        manifest2 = parser.parse()

        assert manifest1 is manifest2


class TestDbtCatalogParser:
    """Tests for DbtCatalogParser class."""

    def test_exists_when_file_exists(self) -> None:
        """Test exists method when file exists."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump({"nodes": {}}, f)
            temp_path = Path(f.name)

        try:
            parser = DbtCatalogParser(temp_path)
            assert parser.exists()
        finally:
            temp_path.unlink()

    def test_exists_when_file_not_exists(self) -> None:
        """Test exists method when file doesn't exist."""
        parser = DbtCatalogParser("/nonexistent/catalog.json")
        assert not parser.exists()

    def test_get_column_info(self) -> None:
        """Test get_column_info method."""
        catalog_data = {
            "nodes": {
                "model.test.my_model": {
                    "columns": {
                        "id": {
                            "name": "id",
                            "type": "INTEGER",
                            "comment": "Primary key",
                            "index": 1,
                        }
                    }
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(catalog_data, f)
            temp_path = Path(f.name)

        try:
            parser = DbtCatalogParser(temp_path)
            columns = parser.get_column_info("model.test.my_model")

            assert "id" in columns
            assert columns["id"]["type"] == "INTEGER"
            assert columns["id"]["comment"] == "Primary key"
        finally:
            temp_path.unlink()


class TestLoadDbtProject:
    """Tests for load_dbt_project function."""

    def test_load_manifest_only(self, sample_manifest_file: Path) -> None:
        """Test loading project with manifest only."""
        manifest = load_dbt_project(sample_manifest_file)

        assert manifest.model_count == 4
        assert "model.test_project.dim_customers" in manifest.models

    def test_load_with_catalog(
        self, sample_manifest_file: Path
    ) -> None:
        """Test loading project with manifest and catalog."""
        catalog_data = {
            "nodes": {
                "model.test_project.dim_customers": {
                    "columns": {
                        "extra_column": {
                            "name": "extra_column",
                            "type": "VARCHAR",
                        }
                    }
                }
            }
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(catalog_data, f)
            catalog_path = Path(f.name)

        try:
            manifest = load_dbt_project(sample_manifest_file, catalog_path)
            model = manifest.get_model_by_name("dim_customers")

            assert model is not None
            # Original columns should still be there
            assert "customer_id" in model.columns
            # Extra column from catalog should be added
            assert "extra_column" in model.columns
        finally:
            catalog_path.unlink()
