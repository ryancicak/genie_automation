#!/usr/bin/env python3
"""
Setup script to configure Databricks workspace for Genie Config Backup automation.

Run this script locally (not in Databricks) to:
1. Create secret scope and store dummy Git PAT
2. Upload backup_genie_config.py to workspace
3. Create the "Genie Config Backup" Job
4. Trigger a test run

Usage:
    python setup_databricks_job.py

Requires: databricks-sdk
    pip install databricks-sdk
"""

import io
import os
import sys
from pathlib import Path

# Add project root for imports
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
WORKSPACE_HOST = os.environ.get("DATABRICKS_HOST")
PAT = os.environ.get("DATABRICKS_TOKEN")

if not WORKSPACE_HOST or not PAT:
    raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env file or environment variables")
SCOPE_NAME = "genie_automation_secrets"
SECRET_KEY = "git-pat"
SECRET_VALUE = "dummy-pat-for-testing"
SPACE_ID = "01f10e9a24f61b178118bb9b90e1b2a9"
BACKUP_SCRIPT_LOCAL = SCRIPT_DIR / "backup_genie_config.py"


def main():
    print("=" * 60)
    print("Databricks Genie Automation Setup")
    print("=" * 60)

    w = WorkspaceClient(host=WORKSPACE_HOST, token=PAT)

    # --- 1. Create Secret Scope ---
    print("\n1. Creating secret scope...")
    try:
        w.secrets.create_scope(scope=SCOPE_NAME)
        print(f"   Created secret scope: {SCOPE_NAME}")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"   Scope '{SCOPE_NAME}' already exists (OK)")
        else:
            print(f"   Scope creation failed: {e}")
            raise

    # --- 2. Store Secret ---
    print("\n2. Storing dummy Git PAT secret...")
    try:
        w.secrets.put_secret(scope=SCOPE_NAME, key=SECRET_KEY, string_value=SECRET_VALUE)
        print(f"   Stored secret '{SECRET_KEY}' in scope '{SCOPE_NAME}'")
        print("   NOTE: Update this with a real GitHub PAT for production!")
    except Exception as e:
        print(f"   Secret storage failed: {e}")
        raise

    # --- 3. Get current user and workspace path ---
    print("\n3. Resolving workspace paths...")
    me = w.current_user.me()
    user_home = f"/Users/{me.user_name}"
    remote_dir = f"{user_home}/genie_automation"
    remote_script_path = f"{remote_dir}/backup_genie_config.py"
    print(f"   User: {me.user_name}")
    print(f"   Remote path: {remote_script_path}")

    # --- 4. Upload script to workspace ---
    print("\n4. Uploading backup script to workspace...")
    if not BACKUP_SCRIPT_LOCAL.exists():
        raise FileNotFoundError(f"Backup script not found: {BACKUP_SCRIPT_LOCAL}")

    w.workspace.mkdirs(path=remote_dir)
    with open(BACKUP_SCRIPT_LOCAL, "rb") as f:
        script_content = f.read()

    w.workspace.upload(
        remote_script_path,
        io.BytesIO(script_content),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    print(f"   Uploaded to {remote_script_path}")

    # --- 5. Create Job ---
    print("\n5. Creating Databricks Job...")
    job = w.jobs.create(
        name="Genie Config Backup (Test)",
        tasks=[
            jobs.Task(
                task_key="backup_task",
                spark_python_task=jobs.SparkPythonTask(
                    python_file=remote_script_path,
                    parameters=[
                        "--space-id",
                        SPACE_ID,
                        "--secret-scope",
                        SCOPE_NAME,
                        "--secret-key",
                        SECRET_KEY,
                    ],
                ),
                new_cluster=compute.ClusterSpec(
                    spark_version="13.3.x-scala2.12",
                    node_type_id="m5d.large",  # AWS node type (workspace is on AWS)
                    num_workers=1,
                ),
            )
        ],
    )
    print(f"   Created Job ID: {job.job_id}")
    print(f"   Job URL: {WORKSPACE_HOST}#job/{job.job_id}")

    # --- 6. Trigger Run ---
    print("\n6. Triggering job run...")
    run = w.jobs.run_now(job_id=job.job_id)
    print(f"   Run ID: {run.run_id}")
    print(f"   Run URL: {WORKSPACE_HOST}#job/{job.job_id}/run/{run.run_id}")

    print("\n" + "=" * 60)
    print("Setup complete!")
    print("=" * 60)
    print("\nExpected behavior:")
    print("  - The job will fetch the Genie config successfully.")
    print("  - Git operations will fail (dummy PAT, no Git repo in workspace).")
    print("  - This confirms the Job setup and config fetch work correctly.")
    print("\nNext steps:")
    print("  1. Update the secret with a real GitHub PAT: databricks secrets put ...")
    print("  2. For full Git backup, run the Job from a Git-backed repo (Git Folder).")
    print("  3. Or clone the repo to workspace and run from there with proper setup.")


if __name__ == "__main__":
    main()
