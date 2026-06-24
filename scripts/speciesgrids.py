import json
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple

import boto3
import botocore
import pyarrow as pa
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class speciesgridsSTACCreator:
    def __init__(self):
        self.s3_bucket = "obis-products"
        self.s3_prefix = "speciesgrids/h3_7/"
        self.keywords = [
            "biodiversity", "marine", "ocean", "OBIS", "GBIF",
            "species", "grids", "H3", "hexagonal", "distributions"
        ]
        self.license = "CC-BY-4.0"
        self.providers = [
            {
                "name": "Ocean Biodiversity Information System (OBIS)",
                "description": "Ocean Biodiversity Information System (OBIS)",
                "roles": ["producer", "processor", "host"],
                "url": "https://obis.org"
            },
            {
                "name": "UNESCO-IOC",
                "description": "Intergovernmental Oceanographic Commission of UNESCO",
                "roles": ["licensor"],
                "url": "https://ioc.unesco.org"
            }
        ]
        self.extent = {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]
            },
            "temporal": {
                # OBIS snapshot: October 2023; GBIF snapshot: May 2024
                "interval": [["2023-10-01T00:00:00Z", "2024-05-31T23:59:59Z"]]
            }
        }
        self.s3_https_base = f"https://{self.s3_bucket}.s3.amazonaws.com"
        self.version = "0.2.0"
        self.s3_client = boto3.client(
            "s3",
            config=boto3.session.Config(signature_version=botocore.UNSIGNED)
        )

    def _list_parquet_keys(self) -> List[str]:
        keys: List[str] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    keys.append(key)
        return sorted(keys)

    def _arrow_type_to_table_type(self, arrow_type: pa.DataType) -> str:
        if pa.types.is_boolean(arrow_type):
            return "boolean"
        if pa.types.is_integer(arrow_type):
            return "integer"
        if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
            return "number"
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "string"
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "binary"
        if pa.types.is_timestamp(arrow_type) or pa.types.is_date(arrow_type):
            return "string"
        if pa.types.is_struct(arrow_type):
            return "struct"
        if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return "list"
        if pa.types.is_map(arrow_type):
            return "map"
        return str(arrow_type)

    def _flatten_schema_fields(self, parent: str, field: pa.Field) -> List[Dict[str, Any]]:
        name = f"{parent}.{field.name}" if parent else field.name
        entries: List[Dict[str, Any]] = [{"name": name, "type": self._arrow_type_to_table_type(field.type)}]
        if pa.types.is_struct(field.type):
            for child in field.type:
                entries.extend(self._flatten_schema_fields(name, child))
        elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            entries.extend(self._flatten_schema_fields(name, field.type.value_field))
        elif pa.types.is_map(field.type):
            entries.extend(self._flatten_schema_fields(name + ".key", field.type.key_field))
            entries.extend(self._flatten_schema_fields(name + ".value", field.type.item_field))
        return entries

    def generate_table_columns(self, parquet_keys: List[str]) -> Tuple[List[Dict[str, Any]], str]:
        parquet_key = parquet_keys[0]
        logger.info(f"Reading schema from s3://{self.s3_bucket}/{parquet_key}")
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=parquet_key)
        data = obj["Body"].read()
        pf = pq.ParquetFile(BytesIO(data))
        schema: pa.Schema = pf.schema_arrow
        columns: List[Dict[str, Any]] = []
        for field in schema:
            columns.extend(self._flatten_schema_fields("", field))
        return columns, parquet_key

    def create_root_catalog_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "obis-speciesgrids-catalog",
            "title": "OBIS speciesgrids catalog",
            "description": (
                "OBIS speciesgrids catalog containing global marine species distributions "
                "aggregated on an H3 hexagonal grid, sourced from OBIS and GBIF."
            ),
            "links": [
                {
                    "rel": "root",
                    "href": "./catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "child",
                    "href": "./speciesgrids-h3-7/catalog.json",
                    "type": "application/json",
                    "title": "speciesgrids H3 resolution 7"
                },
                {
                    "rel": "self",
                    "href": "./catalog.json",
                    "type": "application/json"
                }
            ]
        }

    def create_h3_7_catalog_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "speciesgrids-h3-7",
            "title": "speciesgrids H3 resolution 7",
            "description": (
                "Global marine species distributions from OBIS and GBIF aggregated on an H3 hexagonal grid "
                "at resolution 7 (~5.16 km² cells). The dataset is stored as 64 GeoParquet files partitioned "
                "by Bing Maps quadkey at zoom level 3. Each row represents one species in one H3 cell and "
                "includes occurrence counts, year range, full taxonomy, and IUCN Red List status."
            ),
            "keywords": self.keywords,
            "license": self.license,
            "providers": self.providers,
            "extent": self.extent,
            "properties": {
                "table:columns": []
            },
            "item_assets": {
                "data": {
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": "GeoParquet file",
                    "description": "GeoParquet file partitioned by Bing Maps quadkey at zoom level 3"
                }
            },
            "links": [
                {
                    "rel": "root",
                    "href": "../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "parent",
                    "href": "../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": "./catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/by/4.0/",
                    "type": "text/html",
                    "title": "CC BY 4.0 License"
                },
                {
                    "rel": "documentation",
                    "href": "https://github.com/iobis/speciesgrids",
                    "type": "text/html",
                    "title": "speciesgrids documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            ]
        }

    def create_item_json(self, parquet_keys: List[str]) -> Dict[str, Any]:
        """
        Single STAC item for the full H3-7 dataset.

        The primary 'data' asset points to the S3 prefix for the entire partition set.
        Individual per-quadkey assets are also included for direct file access.
        """
        bbox = self.extent["spatial"]["bbox"][0]

        assets: Dict[str, Any] = {
            "data": {
                "href": f"s3://{self.s3_bucket}/{self.s3_prefix}",
                "type": "application/x-parquet",
                "roles": ["data"],
                "title": "speciesgrids H3-7 full dataset (S3 prefix)",
                "description": (
                    f"Full speciesgrids H3 resolution 7 dataset as 64 GeoParquet files. "
                    f"Partitioned by Bing Maps quadkey at zoom level 3. "
                    f"Access all files under s3://{self.s3_bucket}/{self.s3_prefix}"
                )
            }
        }

        for key in parquet_keys:
            quadkey = Path(key).stem
            assets[f"data-{quadkey}"] = {
                "href": f"{self.s3_https_base}/{key}",
                "type": "application/x-parquet",
                "roles": ["data"],
                "title": f"Quadkey {quadkey}",
                "description": f"GeoParquet file for Bing Maps quadkey {quadkey} (zoom level 3)"
            }

        return {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": "speciesgrids-h3-7",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [bbox[0], bbox[1]],
                    [bbox[2], bbox[1]],
                    [bbox[2], bbox[3]],
                    [bbox[0], bbox[3]],
                    [bbox[0], bbox[1]]
                ]]
            },
            "bbox": bbox,
            "properties": {
                "datetime": "2024-05-31T23:59:59Z",
                "title": "speciesgrids H3 resolution 7",
                "description": (
                    "Global marine species distributions from OBIS (October 2023 snapshot) "
                    "and GBIF (May 2024 snapshot) aggregated on an H3 hexagonal grid at resolution 7. "
                    "Includes per-species occurrence counts, year range, full taxonomy (kingdom through genus), "
                    "IUCN Red List conservation status, and H3 cell centroid geometry in WGS 84."
                ),
                "created": datetime.utcnow().isoformat() + "Z",
                "updated": datetime.utcnow().isoformat() + "Z",
                "version": self.version,
                "table:columns": []
            },
            "assets": assets,
            "links": [
                {
                    "rel": "parent",
                    "href": "../catalog.json",
                    "type": "application/json",
                    "title": "speciesgrids H3-7 catalog"
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": "./speciesgrids-h3-7.json",
                    "type": "application/json"
                },
                {
                    "rel": "about",
                    "href": "https://github.com/iobis/speciesgrids",
                    "type": "text/html",
                    "title": "speciesgrids GitHub repository"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/table/v1.2.0/schema.json",
                "https://stac-extensions.github.io/version/v1.2.0/schema.json"
            ]
        }

    def create_full_catalog(self, output_dir: str) -> Path:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        h3_7_dir = output_path / "speciesgrids-h3-7"
        h3_7_dir.mkdir(exist_ok=True)
        items_dir = h3_7_dir / "items"
        items_dir.mkdir(exist_ok=True)

        parquet_keys = self._list_parquet_keys()
        logger.info(f"Found {len(parquet_keys)} parquet files under s3://{self.s3_bucket}/{self.s3_prefix}")

        root_catalog = self.create_root_catalog_json()
        h3_7_catalog = self.create_h3_7_catalog_json()
        item = self.create_item_json(parquet_keys)

        try:
            table_columns, sampled_key = self.generate_table_columns(parquet_keys)
            h3_7_catalog["properties"]["table:columns"] = table_columns
            item["properties"]["table:columns"] = table_columns
            logger.info(f"Schema introspected from s3://{self.s3_bucket}/{sampled_key}")
        except Exception as e:
            logger.warning(f"Failed to introspect schema: {e}")

        h3_7_catalog["links"].append({
            "rel": "item",
            "href": "./items/speciesgrids-h3-7.json",
            "type": "application/json",
            "title": "speciesgrids H3 resolution 7"
        })

        with open(output_path / "catalog.json", "w") as f:
            json.dump(root_catalog, f, indent=2)

        with open(h3_7_dir / "catalog.json", "w") as f:
            json.dump(h3_7_catalog, f, indent=2)

        with open(items_dir / "speciesgrids-h3-7.json", "w") as f:
            json.dump(item, f, indent=2)

        logger.info(f"STAC catalog written to {output_path}")
        return output_path


def main():
    creator = speciesgridsSTACCreator()
    catalog_path = creator.create_full_catalog(output_dir="./stac/speciesgrids")
    print(f"STAC catalog created at: {catalog_path}")


if __name__ == "__main__":
    main()
