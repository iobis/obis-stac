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
import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class STACCreator:
    def __init__(self):
        self.s3_bucket = "obis-open-data"
        self.s3_prefix = "occurrence/"
        self.keywords = ["biodiversity", "marine", "ocean", "OBIS"]
        self.license = "CC BY-NC 4.0"
        self.providers = [
            {
                "name": "Ocean Biodiversity Information System (OBIS)",
                "description": "Ocean Biodiversity Information System (OBIS)",
                "roles": ["producer", "licensor", "processor", "host"],
                "url": "https://obis.org"
            },
            {
                "name": "UNESCO-IOC",
                "description": "Intergovernmental Oceanographic Commission of UNESCO",
                "roles": ["producer", "licensor", "processor", "host"],
                "url": "https://ioc.unesco.org"
            }
        ]
        self.extent = {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]
            },
            "temporal": {
                "interval": [[None, "2025-12-31T23:59:59Z"]]
            }
        }
        self.s3_client = boto3.client("s3", config=boto3.session.Config(signature_version=botocore.UNSIGNED))

    def _find_first_parquet_key(self) -> str:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    return obj["Key"]
        return ""

    def _list_parquet_keys(self) -> List[str]:
        """List all Parquet object keys under the occurrence prefix."""
        keys: List[str] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    keys.append(key)
        return keys

    def _fetch_datasets_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch dataset metadata from the OBIS API.

        The /dataset endpoint returns all datasets in a single (large) JSON response
        by default, so we just call it once and build a mapping from dataset id
        (which corresponds to the Parquet filename without extension) to its metadata.
        """
        url = "https://api.obis.org/dataset"
        logger.info(f"Fetching dataset metadata from {url}")
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
        except Exception as e:
            logger.warning(f"Failed to fetch dataset metadata from OBIS API: {e}")
            return {}

        datasets: Dict[str, Dict[str, Any]] = {}
        for ds in results:
            ds_id = str(ds.get("id", "")).strip()
            if not ds_id:
                continue
            datasets[ds_id] = ds

        logger.info(f"Fetched metadata for {len(datasets)} datasets from OBIS API")
        return datasets

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
        entries: List[Dict[str, Any]] = []
        entries.append({
            "name": name,
            "type": self._arrow_type_to_table_type(field.type)
        })
        if pa.types.is_struct(field.type):
            for child in field.type:
                entries.extend(self._flatten_schema_fields(name, child))
        elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            value_field = field.type.value_field
            entries.extend(self._flatten_schema_fields(name, value_field))
        elif pa.types.is_map(field.type):
            key_field = field.type.key_field
            item_field = field.type.item_field
            entries.extend(self._flatten_schema_fields(name + ".key", key_field))
            entries.extend(self._flatten_schema_fields(name + ".value", item_field))
        return entries

    def generate_table_columns_from_s3_parquet(self) -> Tuple[List[Dict[str, Any]], str]:
        parquet_key = self._find_first_parquet_key()
        logger.info(f"Reading schema from s3://{self.s3_bucket}/{parquet_key}")
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=parquet_key)
        data = obj["Body"].read()
        pf = pq.ParquetFile(BytesIO(data))
        schema: pa.Schema = pf.schema_arrow
        columns: List[Dict[str, Any]] = []
        for field in schema:
            columns.extend(self._flatten_schema_fields("", field))
        return (columns, parquet_key)

    def create_catalog_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "obis-open-data-catalog",
            "title": "OBIS open data catalog",
            "description": "OBIS open data catalog",
            "links": [
                {
                    "rel": "root",
                    "href": "./catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "child",
                    "href": "./obis-open-data-occurrence-collection/collection.json",
                    "type": "application/json",
                    "title": "OBIS open data occurrence collection"
                },
                {
                    "rel": "self",
                    "href": "./catalog.json",
                    "type": "application/json"
                }
            ]
        }

    def create_collection_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Collection",
            "id": "obis-open-data-occurrence-collection",
            "title": "OBIS open data occurrence collection",
            "description": "OBIS open data occurrence collection",
            "keywords": self.keywords,
            "license": self.license,
            "providers": self.providers,
            "extent": self.extent,
            "sci:citation": "Ocean Biodiversity Information System (OBIS) (25 March 2025) OBIS Occurrence Data. Intergovernmental Oceanographic Commission of UNESCO. https://doi.org/10.25607/obis.occurrence.b89117cd",
            "sci:doi": "10.25607/obis.occurrence.b89117cd",
            "item_assets": {
                "data": {
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": "GeoParquet file",
                    "description": "GeoParquet file"
                }
            },
            "properties": {
                "table:columns": []
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
                    "title": "Parent catalog"
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
                    "href": "https://github.com/iobis/obis-open-data",
                    "type": "text/html",
                    "title": "Dataset documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
            ]
        }

    def create_datasets_collection_json(self) -> Dict[str, Any]:
        """
        Create a child Collection that groups all per-dataset occurrence items.

        This keeps the main occurrence collection focused on the combined dataset,
        while exposing all dataset-level Parquet items in a separate collection.
        """
        return {
            "stac_version": "1.0.0",
            "type": "Collection",
            "id": "obis-open-data-occurrence-datasets",
            "title": "OBIS occurrence individual datasets collection",
            "description": "OBIS occurrence data as individual per dataset GeoParquet files",
            "keywords": self.keywords,
            "license": self.license,
            "providers": self.providers,
            "extent": self.extent,
            "item_assets": {
                "data": {
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": "GeoParquet file",
                    "description": "GeoParquet file for a single OBIS dataset"
                }
            },
            "properties": {
                "table:columns": []
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
                    "href": "../obis-open-data-occurrence-collection/collection.json",
                    "type": "application/json",
                    "title": "OBIS open data occurrence collection"
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
                    "href": "https://github.com/iobis/obis-open-data",
                    "type": "text/html",
                    "title": "Dataset documentation"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
            ]
        }

    def create_combined_item_json(self) -> Dict[str, Any]:
        return {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": "obis-open-data-occurrence-geoparquet",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]],
                    [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][1]],
                    [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][3]],
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][3]],
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]]
                ]]
            },
            "bbox": self.extent["spatial"]["bbox"][0],
            "properties": {
                "datetime": None,
                "start_datetime": self.extent["temporal"]["interval"][0][0],
                "end_datetime": self.extent["temporal"]["interval"][0][1],
                "title": "OBIS full occurrence dataset as GeoParquet",
                "description": "OBIS full occurrence dataset as GeoParquet",
                "created": datetime.utcnow().isoformat() + "Z",
                "updated": datetime.utcnow().isoformat() + "Z",
                "sci:citation": "Ocean Biodiversity Information System (OBIS) (2026) OBIS Occurrence Data. Intergovernmental Oceanographic Commission of UNESCO. https://doi.org/10.25607/obis.occurrence.b89117cd",
                "sci:doi": "10.25607/obis.occurrence.b89117cd"
            },
            "assets": {
                "data": {
                    "href": f"s3://{self.s3_bucket}/occurrence/*",
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": "GeoParquet file",
                    "description": "Geoparquet file"
                }
            },
            "collection": "obis-open-data-occurrence-collection",
            "links": [
                {
                    "rel": "collection",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "Parent collection"
                },
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "Parent collection"
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": f"./obis-occurrence-geoparquet.json",
                    "type": "application/json"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/table/v1.2.0/schema.json",
                "https://stac-extensions.github.io/file/v2.0.0/schema.json"
            ]
        }

    def create_dataset_item_json(self, dataset_meta: Dict[str, Any], parquet_key: str) -> Dict[str, Any]:
        """
        Create a STAC Item for a single OBIS dataset backed by its own Parquet file.

        dataset_meta comes from the OBIS /dataset API (results objects) and should contain:
        - id: dataset identifier (matches Parquet filename without extension)
        - url: dataset URI
        - title: dataset title
        - citation: citation text
        - intellectualrights: license text
        """
        dataset_id = str(dataset_meta.get("id", "")).strip()
        title = dataset_meta.get("title") or f"OBIS dataset {dataset_id}"
        citation = dataset_meta.get("citation") or ""
        citation_id = (dataset_meta.get("citation_id") or "").strip()
        intellectual_rights = dataset_meta.get("intellectualrights") or ""
        dataset_url = dataset_meta.get("url") or ""

        extent_wkt = (dataset_meta.get("extent") or "").strip()
        geometry: Dict[str, Any]
        bbox: List[float]
        if extent_wkt.upper().startswith("POLYGON((") and extent_wkt.endswith("))"):
            try:
                coords_str = extent_wkt[len("POLYGON((") : -2]
                coord_pairs: List[List[float]] = []
                xs: List[float] = []
                ys: List[float] = []
                for pair in coords_str.split(","):
                    parts = pair.strip().split()
                    if len(parts) != 2:
                        continue
                    x, y = float(parts[0]), float(parts[1])
                    coord_pairs.append([x, y])
                    xs.append(x)
                    ys.append(y)
                if coord_pairs:
                    geometry = {
                        "type": "Polygon",
                        "coordinates": [coord_pairs],
                    }
                    bbox = [min(xs), min(ys), max(xs), max(ys)]
                else:
                    raise ValueError("No valid coordinates parsed from extent WKT")
            except Exception as e:
                logger.warning(f"Failed to parse extent WKT for dataset {dataset_id}: {e}")
                geometry = {
                    "type": "Polygon",
                    "coordinates": [[
                        [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]],
                        [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][1]],
                        [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][3]],
                        [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][3]],
                        [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]]
                    ]]
                }
                bbox = self.extent["spatial"]["bbox"][0]
        else:
            geometry = {
                "type": "Polygon",
                "coordinates": [[
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]],
                    [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][1]],
                    [self.extent["spatial"]["bbox"][0][2], self.extent["spatial"]["bbox"][0][3]],
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][3]],
                    [self.extent["spatial"]["bbox"][0][0], self.extent["spatial"]["bbox"][0][1]]
                ]]
            }
            bbox = self.extent["spatial"]["bbox"][0]

        item_id = f"obis-dataset-{dataset_id}"

        item: Dict[str, Any] = {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": item_id,
            "geometry": geometry,
            "bbox": bbox,
            "properties": {
                "datetime": None,
                "start_datetime": self.extent["temporal"]["interval"][0][0],
                "end_datetime": self.extent["temporal"]["interval"][0][1],
                "title": title,
                "description": title,
                "created": datetime.utcnow().isoformat() + "Z",
                "updated": datetime.utcnow().isoformat() + "Z",
                "sci:citation": citation,
                **(
                    {"sci:doi": citation_id.replace("https://doi.org/", "")}
                    if citation_id.startswith("https://doi.org/")
                    else {}
                ),
                "license": intellectual_rights
            },
            "assets": {
                "data": {
                    "href": f"s3://{self.s3_bucket}/{parquet_key}",
                    "type": "application/x-parquet",
                    "roles": ["data"],
                    "title": f"GeoParquet file for dataset {dataset_id}",
                    "description": f"GeoParquet file for OBIS dataset {dataset_id}"
                }
            },
            "collection": "obis-open-data-occurrence-datasets",
            "links": [
                {
                    "rel": "collection",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "Parent collection"
                },
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json",
                    "title": "Parent collection"
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                    "title": "Root catalog"
                },
                {
                    "rel": "self",
                    "href": f"./{item_id}.json",
                    "type": "application/json"
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                "https://stac-extensions.github.io/table/v1.2.0/schema.json",
                "https://stac-extensions.github.io/file/v2.0.0/schema.json"
            ]
        }

        if dataset_url:
            item["links"].append(
                {
                    "rel": "about",
                    "href": dataset_url,
                    "type": "text/html",
                    "title": "Dataset documentation"
                }
            )

        return item

    def create_full_catalog(self, output_dir: str):
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        collection_dir = output_path / "obis-open-data-occurrence-collection"
        collection_dir.mkdir(exist_ok=True)
        items_dir = collection_dir / "items"
        items_dir.mkdir(exist_ok=True)

        datasets_collection_dir = output_path / "obis-open-data-occurrence-datasets"
        datasets_collection_dir.mkdir(exist_ok=True)
        datasets_items_dir = datasets_collection_dir / "items"
        datasets_items_dir.mkdir(exist_ok=True)

        catalog_json = self.create_catalog_json()
        with open(output_path / "catalog.json", "w") as f:
            json.dump(catalog_json, f, indent=2)

        collection_json = self.create_collection_json()
        datasets_collection_json = self.create_datasets_collection_json()

        combined_item_json = self.create_combined_item_json()

        try:
            table_columns, sampled_key = self.generate_table_columns_from_s3_parquet()
            collection_json["properties"]["table:columns"] = table_columns
            datasets_collection_json["properties"]["table:columns"] = table_columns
            combined_item_json.setdefault("properties", {})["table:columns"] = table_columns
            if sampled_key:
                combined_item_json["assets"]["data"]["sample"] = f"s3://{self.s3_bucket}/{sampled_key}"
        except Exception as e:
            logger.warning(f"Failed to auto-generate table:columns: {e}")

        with open(items_dir / f"obis-occurrence-geoparquet.json", "w") as f:
            json.dump(combined_item_json, f, indent=2)

        collection_json["links"].append({
            "rel": "item",
            "href": f"./items/obis-occurrence-geoparquet.json",
            "type": "application/json",
            "title": "OBIS open data occurrence dataset as GeoParquet"
        })

        collection_json["links"].append({
            "rel": "child",
            "href": "../obis-open-data-occurrence-datasets/collection.json",
            "type": "application/json",
            "title": "OBIS occurrence per-dataset collection"
        })

        datasets_meta = self._fetch_datasets_metadata()
        parquet_keys = self._list_parquet_keys()

        dataset_items_created = 0
        for parquet_key in parquet_keys:
            filename = Path(parquet_key).name
            if not filename.endswith(".parquet"):
                continue
            dataset_id = filename[:-8]  # strip ".parquet"

            dataset_meta = datasets_meta.get(dataset_id)
            if not dataset_meta:
                logger.debug(f"No dataset metadata found for id '{dataset_id}', skipping per-dataset item")
                continue

            try:
                dataset_item_json = self.create_dataset_item_json(dataset_meta, parquet_key)
            except Exception as e:
                logger.warning(f"Failed to create STAC item for dataset {dataset_id}: {e}")
                continue

            item_filename = f"obis-dataset-{dataset_id}.json"
            with open(datasets_items_dir / item_filename, "w") as f:
                json.dump(dataset_item_json, f, indent=2)

            datasets_collection_json["links"].append({
                "rel": "item",
                "href": f"./items/{item_filename}",
                "type": "application/json",
                "title": dataset_item_json["properties"].get("title", f"OBIS dataset {dataset_id}")
            })

            dataset_items_created += 1

        with open(collection_dir / "collection.json", "w") as f:
            json.dump(collection_json, f, indent=2)

        with open(datasets_collection_dir / "collection.json", "w") as f:
            json.dump(datasets_collection_json, f, indent=2)

        logger.info(f"STAC catalog created at {output_path}")
        logger.info(
            f"Catalog contains 1 combined item in the main occurrence collection "
            f"and {dataset_items_created} per-dataset items in the datasets collection"
        )

        return output_path


def main():
    creator = STACCreator()
    catalog_path = creator.create_full_catalog(output_dir="./stac")
    print(f"STAC catalog created at: {catalog_path}")


if __name__ == "__main__":
    main()
