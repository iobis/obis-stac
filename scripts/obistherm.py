import json
import logging
import re
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


class ObistherSTACCreator:
    def __init__(self):
        self.s3_bucket = "obis-products"
        self.s3_prefix = "obistherm/"
        self.keywords = [
            "biodiversity", "marine", "ocean", "OBIS",
            "sea surface temperature", "SST", "temperature",
            "species", "occurrences", "GLORYS", "CoralTemp", "MUR-SST", "OSTIA"
        ]
        self.license = "CC-BY-NC-4.0"
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
        self.start_year = 1982
        self.end_year = 2025
        self.extent = {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]
            },
            "temporal": {
                "interval": [[
                    f"{self.start_year}-01-01T00:00:00Z",
                    f"{self.end_year}-12-31T23:59:59Z"
                ]]
            }
        }
        self.s3_https_base = f"https://{self.s3_bucket}.s3.amazonaws.com"
        self.s3_client = boto3.client(
            "s3",
            config=boto3.session.Config(signature_version=botocore.UNSIGNED)
        )

    def _list_parquet_keys(self) -> List[Tuple[int, str]]:
        """Return (year, key) pairs sorted by year."""
        results: List[Tuple[int, str]] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                m = re.search(r"year=(\d{4})", key)
                if m:
                    results.append((int(m.group(1)), key))
        return sorted(results)

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

    def generate_table_columns(self, parquet_keys: List[Tuple[int, str]]) -> List[Dict[str, Any]]:
        if not parquet_keys:
            raise ValueError("No parquet files found")
        # Use the most recent year to read schema quickly
        _, key = sorted(parquet_keys, key=lambda t: t[0], reverse=True)[0]
        logger.info(f"Reading schema from s3://{self.s3_bucket}/{key}")
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
        data = obj["Body"].read()
        schema: pa.Schema = pq.read_schema(BytesIO(data))
        columns: List[Dict[str, Any]] = []
        for field in schema:
            columns.extend(self._flatten_schema_fields("", field))
        return columns

    def create_root_catalog_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "obistherm-catalog",
            "title": "obistherm catalog",
            "description": (
                "Catalog for the obistherm dataset: OBIS marine species occurrence records "
                "matched with monthly sea temperature from GLORYS, CoralTemp, MUR-SST, and OSTIA."
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
                    "href": "./obistherm-full/collection.json",
                    "type": "application/json",
                    "title": "obistherm full dataset"
                },
                {
                    "rel": "child",
                    "href": "./obistherm-by-year/collection.json",
                    "type": "application/json",
                    "title": "obistherm by year"
                },
                {
                    "rel": "self",
                    "href": "./catalog.json",
                    "type": "application/json"
                }
            ]
        }

    def create_full_collection_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Collection",
            "id": "obistherm-full",
            "title": "obistherm full dataset",
            "description": (
                "OBIS occurrence data matched with monthly sea temperature from four satellite "
                "and reanalysis products: GLORYS (CMEMS global ocean reanalysis, 50 depth levels, "
                "1/12° resolution, 1993–present), CoralTemp (NOAA nighttime SST, 5 km, 1986–present), "
                "MUR-SST (NASA daily SST, ~1 km, 2002–present), and OSTIA (Met Office foundation SST, "
                "0.05°, 2007–present). Each record links a marine species occurrence to surface, mid, "
                "deep, and bottom temperatures as well as H3 hexagonal cell membership. "
                f"The dataset covers {self.start_year}–{self.end_year} and is partitioned by year."
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
                    "description": "Full obistherm dataset as GeoParquet, partitioned by year"
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
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/by-nc/4.0/",
                    "type": "text/html",
                    "title": "CC BY-NC 4.0 License"
                },
                {
                    "rel": "documentation",
                    "href": "https://github.com/iobis/obistherm",
                    "type": "text/html",
                    "title": "obistherm documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            ]
        }

    def create_full_item_json(self) -> Dict[str, Any]:
        bbox = self.extent["spatial"]["bbox"][0]
        return {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": "obistherm-full",
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
                "datetime": None,
                "start_datetime": self.extent["temporal"]["interval"][0][0],
                "end_datetime": self.extent["temporal"]["interval"][0][1],
                "title": "obistherm full dataset",
                "description": (
                    f"Full obistherm dataset ({self.start_year}–{self.end_year}): OBIS occurrence records "
                    "matched with sea temperature from GLORYS, CoralTemp, MUR-SST, and OSTIA."
                ),
                "created": datetime.utcnow().isoformat() + "Z",
                "updated": datetime.utcnow().isoformat() + "Z",
                "table:columns": []
            },
            "assets": {
                "data": {
                    "href": f"{self.s3_https_base}/{self.s3_prefix}",
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": "obistherm full dataset GeoParquet",
                    "description": f"Full obistherm dataset as GeoParquet, partitioned by year under {self.s3_prefix}"
                }
            },
            "links": [
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "obistherm full dataset collection"
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": "./obistherm-full.json",
                    "type": "application/json"
                },
                {
                    "rel": "about",
                    "href": "https://github.com/iobis/obistherm",
                    "type": "text/html",
                    "title": "obistherm documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            ]
        }

    def create_byyear_collection_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Collection",
            "id": "obistherm-by-year",
            "title": "obistherm by year",
            "description": (
                "OBIS occurrence data matched with monthly sea temperature from four satellite "
                "and reanalysis products: GLORYS (CMEMS global ocean reanalysis, 50 depth levels, "
                "1/12° resolution, 1993–present), CoralTemp (NOAA nighttime SST, 5 km, 1986–present), "
                "MUR-SST (NASA daily SST, ~1 km, 2002–present), and OSTIA (Met Office foundation SST, "
                "0.05°, 2007–present). Each record links a marine species occurrence to surface, mid, "
                "deep, and bottom temperatures as well as H3 hexagonal cell membership. "
                f"The dataset covers {self.start_year}–{self.end_year} and is partitioned by year."
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
                    "description": "GeoParquet file for a single year, partitioned as year=YYYY/part-0.parquet"
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
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/by-nc/4.0/",
                    "type": "text/html",
                    "title": "CC BY-NC 4.0 License"
                },
                {
                    "rel": "documentation",
                    "href": "https://github.com/iobis/obistherm",
                    "type": "text/html",
                    "title": "obistherm documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            ]
        }

    def create_item_json(self, year: int, parquet_key: str) -> Dict[str, Any]:
        bbox = self.extent["spatial"]["bbox"][0]
        return {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": f"obistherm-{year}",
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
                "datetime": None,
                "start_datetime": f"{year}-01-01T00:00:00Z",
                "end_datetime": f"{year}-12-31T23:59:59Z",
                "title": f"obistherm {year}",
                "description": (
                    f"OBIS occurrence records for {year} matched with sea temperature "
                    "from GLORYS, CoralTemp, MUR-SST, and OSTIA."
                ),
                "created": datetime.utcnow().isoformat() + "Z",
                "updated": datetime.utcnow().isoformat() + "Z",
                "table:columns": []
            },
            "assets": {
                "data": {
                    "href": f"{self.s3_https_base}/{parquet_key}",
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": f"obistherm {year} GeoParquet",
                    "description": f"GeoParquet file for year {year}"
                }
            },
            "links": [
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "obistherm by year collection"
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": f"./obistherm-{year}.json",
                    "type": "application/json"
                },
                {
                    "rel": "about",
                    "href": "https://github.com/iobis/obistherm",
                    "type": "text/html",
                    "title": "obistherm documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            ]
        }

    def create_full_catalog(self, output_dir: str) -> Path:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        full_collection_dir = output_path / "obistherm-full"
        full_collection_dir.mkdir(exist_ok=True)
        full_items_dir = full_collection_dir / "items"
        full_items_dir.mkdir(exist_ok=True)

        byyear_collection_dir = output_path / "obistherm-by-year"
        byyear_collection_dir.mkdir(exist_ok=True)
        byyear_items_dir = byyear_collection_dir / "items"
        byyear_items_dir.mkdir(exist_ok=True)

        parquet_keys = self._list_parquet_keys()
        logger.info(f"Found {len(parquet_keys)} yearly parquet files under s3://{self.s3_bucket}/{self.s3_prefix}")

        root_catalog = self.create_root_catalog_json()
        full_collection = self.create_full_collection_json()
        byyear_collection = self.create_byyear_collection_json()

        try:
            table_columns = self.generate_table_columns(parquet_keys)
            full_collection["properties"]["table:columns"] = table_columns
            byyear_collection["properties"]["table:columns"] = table_columns
            logger.info("Schema introspected successfully")
        except Exception as e:
            logger.warning(f"Failed to introspect schema: {e}")

        full_item = self.create_full_item_json()
        if full_collection["properties"].get("table:columns"):
            full_item["properties"]["table:columns"] = full_collection["properties"]["table:columns"]

        with open(full_items_dir / "obistherm-full.json", "w") as f:
            json.dump(full_item, f, indent=2)

        full_collection["links"].append({
            "rel": "item",
            "href": "./items/obistherm-full.json",
            "type": "application/json",
            "title": "obistherm full dataset"
        })

        for year, key in parquet_keys:
            item = self.create_item_json(year, key)
            if byyear_collection["properties"].get("table:columns"):
                item["properties"]["table:columns"] = byyear_collection["properties"]["table:columns"]

            item_filename = f"obistherm-{year}.json"
            with open(byyear_items_dir / item_filename, "w") as f:
                json.dump(item, f, indent=2)

            byyear_collection["links"].append({
                "rel": "item",
                "href": f"./items/{item_filename}",
                "type": "application/json",
                "title": f"obistherm {year}"
            })

        with open(output_path / "catalog.json", "w") as f:
            json.dump(root_catalog, f, indent=2)

        with open(full_collection_dir / "collection.json", "w") as f:
            json.dump(full_collection, f, indent=2)

        with open(byyear_collection_dir / "collection.json", "w") as f:
            json.dump(byyear_collection, f, indent=2)

        logger.info(
            f"STAC catalog written to {output_path} "
            f"(1 full-dataset item, {len(parquet_keys)} yearly items)"
        )
        return output_path


def main():
    creator = ObistherSTACCreator()
    catalog_path = creator.create_full_catalog(output_dir="./stac/obistherm")
    print(f"STAC catalog created at: {catalog_path}")


if __name__ == "__main__":
    main()
