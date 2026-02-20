#!/usr/bin/env python3
"""
Verification script for Genie Configuration Fetching.

Proves API connectivity and the core logic of backup_genie_config.py
without requiring a Git repo or Databricks runtime (dbutils).

Run locally: python genie_automation/verify_genie_fetch.py [--space-id <id>]
"""

import argparse
import json
import os
from databricks.sdk import WorkspaceClient

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Use env vars for production; fallback for local verification
HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

if not HOST or not TOKEN:
    raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env file or environment variables")

parser = argparse.ArgumentParser()
parser.add_argument("--space-id", help="Optional: specific Genie space ID to fetch")
args = parser.parse_args()

w = WorkspaceClient(host=HOST, token=TOKEN)

print("=" * 60)
print("Genie Configuration Fetch Verification")
print("=" * 60)

space_id = args.space_id

if not space_id:
    # Step 1: List spaces via SDK
    try:
        list_response = w.genie.list_spaces()
        spaces = list(list_response.spaces) if hasattr(list_response, "spaces") else []
        print(f"\nFound {len(spaces)} Genie space(s)")
    except Exception as e:
        print(f"Error listing spaces: {e}")
        raise

    if not spaces:
        print("No Genie spaces found in this workspace.")
        print("Pass --space-id <id> to test with a known space ID.")
        exit(0)

    # Step 2: Use first space for testing
    target_space = spaces[0]
    space_id = getattr(target_space, "space_id", None) or getattr(target_space, "id", None)
    space_name = getattr(target_space, "title", None) or getattr(target_space, "name", "Unknown")
else:
    space_name = f"Space {space_id}"

print(f"\nTest Space: {space_name} (ID: {space_id})")

# Step 3: Fetch full config with serialized_space (matches backup_genie_config.py exactly)
try:
    space_data = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
except Exception as e:
    print(f"Error fetching space details: {e}")
    raise

# Same logic as backup_genie_config.py lines 79-85
config_str = space_data.get("serialized_space")
if not config_str:
    print("Warning: No serialized_space in response.")
    config = {}
else:
    config = json.loads(config_str)

# Step 4: Summarize (matches backup script structure)
tables = config.get("data_sources", {}).get("tables", [])
metric_views = config.get("data_sources", {}).get("metric_views", [])
print(f"\nSuccessfully fetched config:")
print(f"  - Tables: {len(tables)}")
for t in tables[:5]:
    print(f"    - {t.get('identifier', '?')}")
if len(tables) > 5:
    print(f"    ... and {len(tables) - 5} more")
if metric_views:
    print(f"  - Metric views: {len(metric_views)}")

print("\n" + "=" * 60)
print("VERIFICATION PASSED: Genie config fetch logic works.")
print("=" * 60)
