import csv
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Union

import chardet
import requests

# Add logger configuration
logger = logging.getLogger(__name__)


class Table:
    def __init__(self, glide, table_id: str):
        self.glide = glide
        self.table_id = table_id
        self.table_name = f"native-table-{table_id}"
        self._schema = None
        self._rows_cache = None
        # Get table name from list of tables
        self.name = next(
            (t["name"] for t in glide.list_tables() if t["id"] == table_id), None
        )
        self._active_stash = None
        self._stash_threshold = 1000  # Number of rows before using stash

    @property
    def schema(self) -> Dict:
        """Lazy load and cache the table schema"""
        if self._schema is None:
            try:
                response = requests.get(
                    f"{self.glide.DEFAULT_V0_BASE_URL}/{self.glide.app_id}/tables/{self.table_name}/schema",
                    headers=self.glide.headers,
                )
                response.raise_for_status()
                self._schema = response.json()
            except requests.exceptions.RequestException as e:
                raise Exception(f"Failed to get table schema: {str(e)}")
        return self._schema

    def _convert_names_to_ids(self, row_data: Dict) -> Dict:
        """Convert column names to their corresponding IDs"""
        id_mapping = {col["name"]: col["id"] for col in self.schema["data"]["columns"]}
        converted_data = {}
        for key, value in row_data.items():
            if key in id_mapping:
                converted_data[id_mapping[key]] = value
            else:
                converted_data[key] = value  # Keep original ID if not found
        return converted_data

    def _should_use_stash(self, rows: List[Dict], stash: bool) -> bool:
        """Determine if stash should be used based on row count and stash parameter"""
        return stash and len(rows) >= self._stash_threshold

    def _process_with_stash(
        self, rows: List[Dict], operation_fn: callable, **kwargs
    ) -> Dict:
        """Process large datasets using stash functionality

        Args:
            rows: List of row data to process
            operation_fn: Callback function that performs the actual operation
            **kwargs: Additional arguments for the operation
        """
        chunk_size = 1000
        stash = self.glide.create_stash()

        # Add data to stash in chunks
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            stash.add(chunk)

        try:
            # Perform the operation with the stash
            return operation_fn(rows=[], stash_id=stash.stash_id, **kwargs)
        finally:
            # Clean up stash
            stash.delete()

    def add_rows(
        self,
        rows: List[Dict],
        on_schema_error: Optional[str] = None,
        stash: bool = False,
    ) -> Dict:
        """Add multiple rows to the table using either column names or IDs"""
        converted_rows = [self._convert_names_to_ids(row) for row in rows]

        if self._should_use_stash(converted_rows, stash):
            return self._process_with_stash(
                converted_rows,
                lambda **kwargs: self.glide.add_rows(self.table_id, **kwargs),
                on_schema_error=on_schema_error,
            )

        return self.glide.add_rows(self.table_id, converted_rows, on_schema_error)

    def _get_rows_cache(self) -> Dict:
        """Get the rows cache, fetching if needed"""
        if self._rows_cache is None:
            response = self.rows()
            self._rows_cache = {row["$rowID"]: row for row in response[0]["rows"]}
        return self._rows_cache

    def update_row(
        self,
        row_id: str,
        data: Dict,
        on_schema_error: Optional[str] = None,
        force: bool = True,
    ) -> Dict:
        """Update a specific row in the table using either column names or IDs

        Args:
            force (bool): If True, updates row without checking for changes.
                         If False, only updates if values have changed.
        """
        converted_data = self._convert_names_to_ids(data)

        if not force:
            # Check if any values have actually changed using cached data
            current_row = self._get_rows_cache().get(row_id)

            if current_row:
                has_changes = False
                for key, new_value in converted_data.items():
                    if key in current_row and current_row[key] != new_value:
                        has_changes = True
                        break

                if not has_changes:
                    return {"message": "No changes detected", "updated": False}

        result = self.glide.update_row(
            self.table_id, row_id, converted_data, on_schema_error
        )

        # Update cache if the update was successful
        if result.get("success", False):
            if self._rows_cache is not None:
                self._rows_cache[row_id] = {
                    **self._rows_cache.get(row_id, {}),
                    **converted_data,
                }

        return result

    def overwrite(
        self,
        rows: List[Dict],
        on_schema_error: Optional[str] = None,
        stash: bool = False,
    ) -> Dict:
        """Overwrite all data in the table"""
        converted_rows = [self._convert_names_to_ids(row) for row in rows]

        if self._should_use_stash(converted_rows, stash):
            return self._process_with_stash(
                converted_rows,
                lambda **kwargs: self.glide.overwrite_table(self.table_id, **kwargs),
                on_schema_error=on_schema_error,
            )

        return self.glide.overwrite_table(
            self.table_id, converted_rows, on_schema_error
        )

    def rows(self, utc: bool = True) -> Dict:
        """Alias for get_rows() - retrieves all rows from the table

        Args:
            utc (bool, optional): Whether to return timestamps in UTC. Defaults to True.

        Returns:
            Dict: Response data containing the table rows
        """
        return self.glide.get_rows(table_id=self.table_id, utc=utc)

    def __str__(self) -> str:
        return self.name or self.table_id

    def __repr__(self) -> str:
        return f"Table(glide={self.glide!r}, table_id='{self.table_id}')"

    def upsert(
        self,
        rows: List[Dict],
        key: str,
        on_schema_error: Optional[str] = None,
        force: bool = True,
        stash: bool = False,
    ) -> Dict:
        """Upsert rows into the table based on a matching key"""
        logger.debug(f"Starting upsert operation with {len(rows)} rows")

        # Convert key name to ID if necessary
        key_id = key
        if key in {col["name"] for col in self.schema["data"]["columns"]}:
            key_id = next(
                col["id"]
                for col in self.schema["data"]["columns"]
                if col["name"] == key
            )
        elif key not in {col["id"] for col in self.schema["data"]["columns"]}:
            raise ValueError(f"Column '{key}' not found in table schema")

        # Get existing rows
        existing_rows = self.rows()[0]["rows"]
        existing_keys = {row[key_id]: row["$rowID"] for row in existing_rows}
        existing_rows_dict = {row[key_id]: row for row in existing_rows}

        # Debug logging for schema mapping
        logger.debug("Column name to ID mapping:")
        for col in self.schema["data"]["columns"]:
            logger.debug(f"{col['name']} -> {col['id']}")

        # Separate rows into updates and inserts
        updates = []
        inserts = []
        skipped = 0

        for row in rows:
            converted_row = self._convert_names_to_ids(row)
            key_value = converted_row.get(key_id)

            logger.debug(f"Processing row with key value: {key_value}")
            logger.debug(f"Original row data: {row}")
            logger.debug(f"Converted row data: {converted_row}")

            if key_value in existing_keys:
                current_row = existing_rows_dict[key_value]
                logger.debug(f"Found existing row: {current_row}")

                if not force:
                    has_changes = False
                    for k, new_value in converted_row.items():
                        old_value = current_row.get(k)
                        old_is_empty = old_value is None or old_value == ""
                        new_is_empty = new_value is None or new_value == ""

                        if (
                            not (old_is_empty and new_is_empty)
                            and old_value != new_value
                        ):
                            logger.debug(
                                f"Change detected in field {k}: {old_value} -> {new_value}"
                            )
                            has_changes = True

                    if not has_changes:
                        logger.debug("No changes detected, skipping update")
                        skipped += 1
                        continue

                logger.debug(
                    f"Updating row {existing_keys[key_value]} with data: {converted_row}"
                )
                update_result = self.update_row(
                    existing_keys[key_value],
                    converted_row,
                    on_schema_error,
                    force=force,
                )
                logger.debug(f"Update result: {update_result}")
                updates.append(converted_row)
            else:
                logger.debug("No existing row found, adding to inserts")
                inserts.append(converted_row)

        logger.debug(f"""
        Upsert operation completed:
        - Updates prepared: {len(updates)}
        - Inserts prepared: {len(inserts)}
        - Rows skipped (no changes): {skipped}
        """)

        # Bulk insert new rows with stash support if needed
        insert_result = None
        if inserts:
            if self._should_use_stash(inserts, stash):
                logger.info(f"Using stash for {len(inserts)} inserts")
                insert_result = self._process_with_stash(
                    inserts,
                    lambda **kwargs: self.glide.add_rows(self.table_id, **kwargs),
                    on_schema_error=on_schema_error,
                )
            else:
                logger.info(f"Directly inserting {len(inserts)} rows")
                insert_result = self.add_rows(inserts, on_schema_error)

        result = {
            "updated": len(updates),
            "inserted": len(inserts),
            "skipped": skipped,
            "insert_result": insert_result,
        }

        logger.info(f"""
        Final results:
        - Updates completed: {result['updated']}
        - Inserts completed: {result['inserted']}
        - Rows skipped: {result['skipped']}
        """)

        return result

    def upload_csv(
        self,
        file_path: Union[str, Path],
        key_column: Optional[str] = None,
        on_schema_error: Optional[str] = None,
        force: bool = False,
        stash: bool = True,
        encoding: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> Dict:
        """Upload data from a CSV file to the table, using only matching column names"""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # Detect encoding if not provided
        if encoding is None:
            with open(file_path, "rb") as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                encoding = result["encoding"]
                confidence = result["confidence"]
                logger.debug(
                    f"Detected encoding: {encoding} (confidence: {confidence:.2%})"
                )

        try:
            rows = []
            with open(file_path, "r", encoding=encoding) as f:
                reader = csv.DictReader(f)

                # Find matching columns
                csv_headers = set(reader.fieldnames or [])
                schema_columns = {col["name"] for col in self.schema["data"]["columns"]}
                matching_columns = csv_headers & schema_columns  # Intersection of sets

                logger.debug(f"CSV columns: {csv_headers}")
                logger.debug(f"Table columns: {schema_columns}")
                logger.info(f"Matching columns that will be used: {matching_columns}")

                if not matching_columns:
                    raise ValueError("No matching columns found between CSV and table")

                # Process rows using only matching columns
                for row_num, row in enumerate(reader, start=1):
                    filtered_row = {
                        col: row[col]
                        for col in matching_columns
                        if row.get(col) is not None and row[col] != ""
                    }

                    if filtered_row:  # Only add if there's data after filtering
                        rows.append(filtered_row)
                    else:
                        logger.debug(
                            f"Skipping row {row_num} (no valid data in matching columns)"
                        )

        except UnicodeDecodeError:
            logger.error(f"Failed to read CSV with detected encoding {encoding}")
            logger.debug("Falling back to utf-8 with error handling...")
            # ... fallback code if needed ...

        if not rows:
            return {"message": "No valid data found in CSV", "processed": 0}

        logger.info(f"Processing {len(rows)} rows from CSV")

        try:
            if key_column:
                if key_column not in matching_columns:
                    raise ValueError(
                        f"Key column '{key_column}' not found in matching columns"
                    )

                result = self.upsert(
                    rows=rows,
                    key=key_column,
                    on_schema_error=on_schema_error,
                    force=force,
                    stash=stash,
                )
                return {
                    "operation": "upsert",
                    "updated": result["updated"],
                    "inserted": result["inserted"],
                    "skipped": result["skipped"],
                    "total_processed": result["updated"]
                    + result["inserted"]
                    + result["skipped"],
                    "columns_used": list(matching_columns),
                }
            else:
                result = self.add_rows(
                    rows=rows, on_schema_error=on_schema_error, stash=stash
                )
                return {
                    "operation": "add_rows",
                    "processed": len(rows),
                    "result": result,
                    "columns_used": list(matching_columns),
                }

        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            sample_size = min(5, len(rows))
            logger.error(f"Sample of first {sample_size} rows:")
            for i in range(sample_size):
                logger.error(f"Row {i+1}: {rows[i]}")
            raise


class Stash:
    """Handles stashing operations for large dataset imports"""

    def __init__(self, glide):
        """Initialize a new stash with a unique ID"""
        self.glide = glide
        self.stash_id = str(uuid.uuid4())
        self._serial = 0  # Track serial number for chunks

    def add(self, rows: List[Dict]) -> Dict:
        """Add rows to the stash"""
        self._serial += 1

        logger.info(
            f"Adding chunk {self._serial} to stash {self.stash_id} ({len(rows)} rows)"
        )

        try:
            response = requests.put(
                f"{self.glide.base_url}/stashes/{self.stash_id}/{self._serial}",
                headers=self.glide.headers,
                json=rows,
            )

            if not response.ok:
                logger.error(f"Stash API Error Response: {response.text}")

            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully added chunk {self._serial} to stash")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to add chunk {self._serial} to stash: {str(e)}")
            raise

    def delete(self) -> None:
        """Delete the stash"""
        response = requests.delete(
            f"{self.glide.base_url}/stashes/{self.stash_id}", headers=self.glide.headers
        )
        response.raise_for_status()


class Glide:
    DEFAULT_BASE_URL = "https://api.glideapps.com"
    DEFAULT_V1_BASE_URL = "https://api.glideapp.io/api/function"
    DEFAULT_V0_BASE_URL = "https://functions.prod.internal.glideapps.com/api/apps"

    def __init__(
        self,
        auth_token: Optional[str] = None,
        base_url: Optional[str] = None,
        base_url_v1: Optional[str] = None,
        base_url_v0: Optional[str] = None,
        app_id: Optional[str] = None,
    ):
        """
        Initialize the Glide Tables API wrapper

        Args:
            auth_token (str): Your Glide API authentication token or environment variable GLIDE_API_TOKEN
            base_url (Optional[str]): Custom API base URL. If None, uses default.
        """
        self.auth_token = auth_token or os.getenv("GLIDE_API_TOKEN")
        if not self.auth_token:
            raise ValueError(
                "auth_token must be provided or GLIDE_API_TOKEN environment variable must be set"
            )

        self.base_url = (
            base_url or os.getenv("GLIDE_API_BASE_URL") or self.DEFAULT_BASE_URL
        )

        self.app_id = app_id or os.getenv("APP_ID")

        self.headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }

    # V2 API Methods
    def list_tables(self) -> List[Dict]:
        """
        [V2 API] Get all Big Tables in the current team

        Returns:
            List[Dict]: A collection of table objects, each with 'id' and 'name'

        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        try:
            response = requests.get(f"{self.base_url}/tables", headers=self.headers)
            response.raise_for_status()
            return response.json()["data"]

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch tables: {str(e)}")

    def create_table(
        self,
        name: str,
        rows: List[Dict] = [],
        schema: Optional[Dict] = None,
        on_schema_error: Optional[str] = None,
    ) -> Dict:
        """
        [V2 API] Create a new Big Table in Glide

        Args:
            name (str): Name of the table (e.g., 'Invoices')
            rows (List[Dict]): Collection of row objects conforming to the schema (defaults to empty list)
            schema (Optional[Dict]): Schema definition for the table. If not provided,
                will be inferred from the data
            on_schema_error (Optional[str]): Action to take when data doesn't match schema.
                Options: 'abort', 'dropColumns', 'updateSchema'

        Returns:
            Dict: The created table object

        Raises:
            requests.exceptions.RequestException: If the API request fails
            ValueError: If invalid parameters are provided
        """
        if on_schema_error and on_schema_error not in [
            "abort",
            "dropColumns",
            "updateSchema",
        ]:
            raise ValueError(
                "on_schema_error must be one of: abort, dropColumns, updateSchema"
            )

        payload = {"name": name, "rows": rows}
        if schema:
            payload["schema"] = schema

        params = {}
        if on_schema_error:
            params["onSchemaError"] = on_schema_error

        try:
            response = requests.post(
                f"{self.base_url}/tables",
                headers=self.headers,
                json=payload,
                params=params,
            )
            breakpoint()
            response.raise_for_status()
            return response.json()["data"]

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to create table: {str(e)}")

    def add_rows(
        self,
        table_id: str,
        rows: List[Dict],
        on_schema_error: Optional[str] = None,
        stash_id: Optional[str] = None,
    ) -> Dict:
        """[V2 API] Add rows to an existing Big Table"""
        if on_schema_error and on_schema_error not in [
            "abort",
            "dropColumns",
            "updateSchema",
        ]:
            raise ValueError(
                "on_schema_error must be one of: abort, dropColumns, updateSchema"
            )

        params = {}
        if on_schema_error:
            params["onSchemaError"] = on_schema_error

        if stash_id:
            payload = {"$stashID": stash_id}
            logger.info(f"Adding rows from stash {stash_id}")
        else:
            payload = rows
            logger.info(f"Adding {len(rows)} rows directly")

        try:
            response = requests.post(
                f"{self.base_url}/tables/{table_id}/rows",
                headers=self.headers,
                json=payload,
                params=params,
            )

            if not response.ok:
                logger.error(f"API Error Response: {response.text}")

            response.raise_for_status()
            result = response.json()["data"]

            # Log the results
            if "rowCount" in result:
                logger.info(f"Successfully added {result['rowCount']} rows")

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            logger.error(
                f"Response content: {e.response.text if hasattr(e, 'response') else 'No response content'}"
            )
            raise Exception(f"Failed to add rows: {str(e)}")

    def overwrite_table(
        self,
        table_id: str,
        rows: List[Dict],
        on_schema_error: Optional[str] = None,
        stash_id: Optional[str] = None,
    ) -> Dict:
        """[V2 API] Overwrite all data in a Big Table"""
        if on_schema_error and on_schema_error not in [
            "abort",
            "dropColumns",
            "updateSchema",
        ]:
            raise ValueError(
                "on_schema_error must be one of: abort, dropColumns, updateSchema"
            )

        params = {}
        if on_schema_error:
            params["onSchemaError"] = on_schema_error
        if stash_id:
            params["stashID"] = stash_id

        try:
            response = requests.put(
                f"{self.base_url}/tables/{table_id}",
                headers=self.headers,
                json={"rows": rows},
                params=params,
            )
            response.raise_for_status()
            return response.json()["data"]

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to overwrite table: {str(e)}")

    def update_row(
        self,
        table_id: str,
        row_id: str,
        data: Dict,
        on_schema_error: Optional[str] = None,
    ) -> Dict:
        """
        [V2 API] Update a specific row in a Big Table

        Args:
            table_id (str): ID of the table containing the row
            row_id (str): ID of the row to update
            data (Dict): Updated row data
            on_schema_error (Optional[str]): Action to take when data doesn't match schema.
                Options: 'abort', 'dropColumns', 'updateSchema'

        Returns:
            Dict: Response data from the API

        Raises:
            requests.exceptions.RequestException: If the API request fails
            ValueError: If invalid parameters are provided
        """
        if on_schema_error and on_schema_error not in [
            "abort",
            "dropColumns",
            "updateSchema",
        ]:
            raise ValueError(
                "on_schema_error must be one of: abort, dropColumns, updateSchema"
            )

        params = {}
        if on_schema_error:
            params["onSchemaError"] = on_schema_error

        try:
            response = requests.patch(
                f"{self.base_url}/tables/{table_id}/rows/{row_id}",
                headers=self.headers,
                json=data,
                params=params,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to update row: {str(e)}")

    # V1 API Methods (Legacy)
    def get_rows(
        self,
        table_id: Optional[str] = None,
        table_name: Optional[str] = None,
        utc: bool = True,
    ) -> Dict:
        """
        [V1 API - DEPRECATED] Get rows from a table (requires Business plan or above)

        Note: This method uses the legacy V1 API and will be replaced with a V2 variant
        in the future.

        Args:
            table_id (Optional[str]): ID of the table
            table_name (Optional[str]): Name of the table
            utc (bool): Whether to return timestamps in UTC (defaults to True)

        Returns:
            Dict: Response data containing the table rows

        Raises:
            requests.exceptions.RequestException: If the API request fails
            ValueError: If app_id is not set or if neither table_id nor table_name is provided
        """
        if not self.app_id:
            raise ValueError(
                "app_id must be provided during initialization or set as APP_ID environment variable"
            )

        if not table_id and not table_name:
            raise ValueError("Either table_id or table_name must be provided")

        if table_id:
            table_name = f"native-table-{table_id}"

        payload = {
            "appID": self.app_id,
            "queries": [{"tableName": table_name, "utc": utc}],
        }

        try:
            response = requests.post(
                f"{self.DEFAULT_V1_BASE_URL}/queryTables",
                headers=self.headers,
                json=payload,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get rows: {str(e)}")

    def table(self, table_identifier: str) -> Table:
        """
        Get a Table object for the specified table name or ID

        Args:
            table_identifier (str): Name or ID of the table to interact with

        Returns:
            Table: A Table object for performing operations on the specified table

        Raises:
            ValueError: If no table is found matching the provided name or ID
        """
        # Get all tables and find the matching one
        tables = self.list_tables()
        for table in tables:
            if table_identifier in (table["name"], table["id"]):
                return Table(self, table["id"])
        raise ValueError(f"Table with name or ID '{table_identifier}' not found")

    @property
    def tables(self) -> List[Dict]:
        """
        Alias for list_tables()

        Returns:
            List[Dict]: A collection of table objects, each with 'id' and 'name'
        """
        return self.list_tables()

    def __str__(self) -> str:
        return f"Glide(base_url={self.base_url})"

    def create_stash(self) -> Stash:
        """Create a new stash"""
        return Stash(self)
