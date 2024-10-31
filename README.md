# 🚀 Glide API Wrapper

A Python wrapper for the [Glide API](https://apidocs.glideapps.com/) with some extra convenience functions to make your life a little easier.

> [Glide](https://www.glideapps.com/) is a no code app builder.

## 🔧 Installation

```bash
# Clone the repository
git clone https://github.com/McKinnonIT/glide.py.git
cd glide.py

# Using uv
uv pip install -r requirements.txt

# Using pip
pip install -r requirements.txt
```

## 🏃‍♂️ Quick Start

```python
from glide.glide import Glide

# Initialize with your API token
glide = Glide(auth_token="00000000-0000-0000-0000-000000000000") 

# OR with the GLIDE_API_TOKEN envrionment variable
glide = Glide()

# Display all table names and IDs
glide.tables

# Get a table
table = glide.table("Your Table Name")

# Get table data (rows)
table.rows()

# Add some rows
table.add_rows([
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
])
```

## 🔄 Native API Methods

These methods directly map to Glide's V2 API endpoints:

- `list_tables()` - Get all tables
- `create_table(name, rows=[], schema=None)` - Create a new table
- `add_rows(table_id, rows)` - Add rows to a table
- `update_row(table_id, row_id, data)` - Update a specific row
- `overwrite_table(table_id, rows)` - Replace all data in a table

## ✨ Helper Methods

I've added some convenience methods to make common operations easier:

### 📊 Table Class
- `upsert(rows, key)` - Update existing rows or insert new ones based on a key column
- `upload_csv(file_path, key_column=None)` - Import data from a CSV file
- `_convert_names_to_ids()` - Automatically converts column names to Glide's internal IDs

### ⚠️ Method Limitations

#### upsert()
- Updates are performed one row at a time due to API limitations
- Can be slow for large datasets with many updates (new rows are still batch processed)
- Consider using `add_rows()` if you don't need to check for duplicates

#### upload_csv()
- Only processes columns that exist in both the CSV and your Glide table
- Silently ignores CSV columns that don't match your table schema

### 📦 Stash Support
For large datasets, the wrapper automatically handles stashing:
- Automatically chunks large datasets
- Uses Glide's stash API for better performance
- Handles cleanup after operations complete

## 🔑 Environment Variables

- `GLIDE_API_TOKEN` - Your Glide API token
- `GLIDE_API_BASE_URL` - Optional custom base URL for the main API
- `GLIDE_API_V1_BASE_URL` - Optional custom base URL for V1 API endpoints
- `GLIDE_API_V0_BASE_URL` - Optional custom base URL for V0 API endpoints
- `APP_ID` - Required for some legacy V1 API operations

## 📄 License

MIT - Do whatever you want! 🎉

---

Note: This wrapper includes both V2 (current) and V1 (legacy) API support. V1 methods are marked as deprecated and should be avoided in new code.
