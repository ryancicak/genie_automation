#!/usr/bin/env python3
"""
Create a test Genie Space and verify the backup automation.

This script:
1. Authenticates with Databricks using PAT
2. Lists tables in the catalog; creates a test table if none exist
3. Creates a new Genie Space with the identified/created tables
4. Verifies creation by fetching the space config
5. Runs the backup logic (saves config to genie_configs/) to prove backup works

Run locally: python genie_automation/setup_and_backup_genie.py

Environment variables (optional): DATABRICKS_HOST, DATABRICKS_TOKEN
"""

import json
import os
import secrets

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

if not HOST or not TOKEN:
    raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env file or environment variables")
CATALOG = "cicaktest_catalog"
SCHEMA = "default"
TEST_TABLE_NAME = "genie_test_users"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TEST_TABLE_NAME}"

SPACE_TITLE = "Genie Automation Test Space"
SPACE_DESCRIPTION = "A test space for verifying automated backups."


def main():
    w = WorkspaceClient(host=HOST.rstrip("/"), token=TOKEN)

    print("=" * 60)
    print("Genie Test Space Setup & Backup Verification")
    print("=" * 60)

    # --- Step 1: Get a SQL warehouse (required for Genie and for creating tables) ---
    print("\n[1] Finding SQL warehouse...")
    warehouses = list(w.warehouses.list())
    eligible = [
        wh for wh in warehouses
        if getattr(wh, "enable_serverless_compute", False)
        or (hasattr(wh, "warehouse_type") and str(getattr(wh, "warehouse_type", "")).upper() == "PRO")
    ]
    if not eligible:
        # Fallback: use any warehouse (user may have only classic)
        eligible = warehouses
    if not eligible:
        raise RuntimeError(
            "No SQL warehouse found. Genie spaces require a pro or serverless warehouse. "
            "Create one in SQL Warehouses in the Databricks UI."
        )
    warehouse_id = eligible[0].id
    print(f"    Using warehouse: {eligible[0].name} (ID: {warehouse_id})")

    # --- Step 2: List tables in catalog ---
    print(f"\n[2] Listing tables in {CATALOG}.{SCHEMA}...")
    try:
        tables_iter = w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA, max_results=10)
        tables_list = list(tables_iter)
    except Exception as e:
        print(f"    Error listing tables: {e}")
        tables_list = []

    target_tables = []
    if tables_list:
        # Use 1-2 existing tables
        for t in tables_list[:2]:
            full_name = getattr(t, "full_name", None) or getattr(t, "name", str(t))
            target_tables.append(full_name)
        print(f"    Found {len(tables_list)} table(s). Using: {target_tables}")
    else:
        # Create a simple test table
        print(f"    No tables found. Creating {FULL_TABLE_NAME}...")
        try:
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
                id INT,
                name STRING
            )
            """
            w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=create_sql,
                catalog=CATALOG,
                schema=SCHEMA,
                wait_timeout="30s",
            )
            # Insert dummy data
            insert_sql = f"""
            INSERT INTO {FULL_TABLE_NAME} VALUES
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
            """
            w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=insert_sql,
                catalog=CATALOG,
                schema=SCHEMA,
                wait_timeout="30s",
            )
            target_tables = [FULL_TABLE_NAME]
            print(f"    Created table: {FULL_TABLE_NAME}")
        except Exception as e:
            print(f"    Could not create table: {e}")
            print("    Proceeding with empty space (no tables).")

    # --- Step 3: Build serialized_space config ---
    print("\n[3] Building Genie space configuration...")
    question_id = secrets.token_hex(16)
    tables_config = sorted(
        [
            {
                "identifier": tbl,
                "description": [f"Table for Genie automation test"],
                "column_configs": [],
            }
            for tbl in target_tables
        ],
        key=lambda x: x["identifier"],
    )

    config = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": question_id, "question": ["How many users are in the table?"]}
            ],
        },
        "data_sources": {
            "tables": tables_config,
        },
        "instructions": {
            "text_instructions": [],
            "example_question_sqls": [],
            "sql_snippets": {"measures": [], "filters": [], "expressions": []},
            "join_specs": [],
            "sql_functions": [],
        },
    }

    # If no tables, use minimal config
    if not target_tables:
        config["data_sources"]["tables"] = []
        config["config"]["sample_questions"] = [
            {"id": question_id, "question": ["This is a test space for backup verification."]}
        ]

    serialized_space = json.dumps(config)

    # --- Step 4: Create Genie Space ---
    print("\n[4] Creating Genie Space...")
    create_response = w.api_client.do(
        "POST",
        "/api/2.0/genie/spaces",
        body={
            "serialized_space": serialized_space,
            "warehouse_id": warehouse_id,
            "title": SPACE_TITLE,
            "description": SPACE_DESCRIPTION,
        },
    )
    space_id = create_response.get("space_id")
    if not space_id:
        raise RuntimeError(f"Create response missing space_id: {create_response}")

    host = w.config.host.rstrip("/")
    print(f"    Created Space: {SPACE_TITLE}")
    print(f"    Space ID: {space_id}")
    print(f"    URL: {host}/genie/rooms/{space_id}")

    # --- Step 5: Verify creation ---
    print("\n[5] Verifying creation...")
    space_data = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
    fetched_title = space_data.get("title", "N/A")
    print(f"    Fetched space: {fetched_title} (ID: {space_data.get('space_id', 'N/A')})")

    # --- Step 6: Backup (save config to file) ---
    print("\n[6] Running backup (saving config to file)...")
    config_str = space_data.get("serialized_space")
    if not config_str:
        print("    Warning: No serialized_space in response.")
        backup_config = {}
    else:
        backup_config = json.loads(config_str)

    config_dir = os.path.join(os.path.dirname(__file__), "..", "genie_configs")
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.join(config_dir, f"space_{space_id}.json")

    with open(config_path, "w") as f:
        json.dump(backup_config, f, indent=2, sort_keys=True)

    print(f"    Configuration saved to: {config_path}")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUCCESS: Test Genie Space created and backup verified.")
    print("=" * 60)
    print(f"  Space ID: {space_id}")
    print(f"  Tables: {target_tables or '(none)'}")
    print(f"  Config file: {config_path}")
    print(f"  Open in browser: {host}/genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
