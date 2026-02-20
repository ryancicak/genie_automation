"""
Automated Backup of Genie Space Configuration to Git

This script is designed to be run as a Databricks Job (Task Type: Python Script).
It fetches the current configuration of a Genie Space and commits it to the Git repository.

Prerequisites:
1.  Service Principal has a Git PAT registered (see references/git_automation.md).
2.  Databricks Secret scope and key for the Git PAT are configured.
3.  The Job is configured to run this script from a Databricks Git Folder.

Usage:
    Run as a Job with the following parameters (as command-line arguments):
    --space-id <space_id>
    --secret-scope <scope>
    --secret-key <key>
    [--git-username <username>]
    [--git-email <email>]
"""

import json
import os
import subprocess
import argparse
import sys
import tempfile
import shutil
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils

def parse_args():
    parser = argparse.ArgumentParser(description="Backup Genie Space Configuration")
    parser.add_argument("--space-id", required=True, help="Genie Space ID")
    parser.add_argument("--secret-scope", required=True, help="Secret Scope for Git PAT")
    parser.add_argument("--secret-key", required=True, help="Secret Key for Git PAT")
    parser.add_argument("--git-username", default="genie-backup-bot", help="Git Username")
    parser.add_argument("--git-email", default="bot@company.com", help="Git Email")
    return parser.parse_args()

def run_git_cmd(cmd, cwd, safe_cmd=None):
    """Helper to run git commands"""
    print(f"Running: {safe_cmd or cmd}")
    result = subprocess.run(
        cmd, 
        cwd=cwd, 
        shell=True, 
        capture_output=True, 
        text=True
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception(f"Git command failed: {safe_cmd or cmd}")
    print(result.stdout)
    return result

def main():
    args = parse_args()
    
    SPACE_ID = args.space_id
    SECRET_SCOPE = args.secret_scope
    SECRET_KEY = args.secret_key
    GIT_USERNAME = args.git_username
    GIT_EMAIL = args.git_email
    REPO_URL = "https://github.com/ryancicak/genie_automation.git"

    # --- 1. Fetch Genie Configuration ---
    print(f"Fetching configuration for Genie Space: {SPACE_ID}...")
    w = WorkspaceClient()

    try:
        # Fetch the space details
        # The SDK returns an object, we need to access the serialized_space field
        # Note: The SDK might not expose serialized_space directly if it's not in the model
        # Let's use the underlying API client to be sure we get the raw JSON
        raw_response = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{SPACE_ID}",
            query={"include_serialized_space": "true"},
        )
        
        current_config_str = raw_response.get("serialized_space")
        if not current_config_str:
            print("Warning: No serialized_space found in response.")
            current_config = {}
        else:
            current_config = json.loads(current_config_str)

    except Exception as e:
        print(f"Error fetching Genie configuration: {e}")
        raise

    # --- 2. Git Operations ---
    print("Starting Git operations in temporary directory...")

    # Retrieve the Git PAT securely
    try:
        git_token = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temp directory: {temp_dir}")
        
        # Clone repo
        auth_repo_url = REPO_URL.replace("https://", f"https://{GIT_USERNAME}:{git_token}@")
        safe_repo_url = REPO_URL.replace("https://", f"https://{GIT_USERNAME}:***@")
        
        try:
            run_git_cmd(f"git clone {auth_repo_url} .", temp_dir, safe_cmd=f"git clone {safe_repo_url} .")
            
            # Configure Git
            run_git_cmd(f"git config user.email '{GIT_EMAIL}'", temp_dir)
            run_git_cmd(f"git config user.name '{GIT_USERNAME}'", temp_dir)
            
            # Save config
            config_dir = os.path.join(temp_dir, "genie_configs")
            os.makedirs(config_dir, exist_ok=True)
            config_filename = os.path.join(config_dir, f"space_{SPACE_ID}.json")
            
            with open(config_filename, "w") as f:
                json.dump(current_config, f, indent=2, sort_keys=True)
            print(f"Saved config to {config_filename}")
            
            # Commit and Push
            status = run_git_cmd("git status --porcelain", temp_dir)
            if status.stdout.strip():
                print("Changes detected. Committing...")
                run_git_cmd("git add .", temp_dir)
                run_git_cmd(f"git commit -m 'Backup: Automated Genie config update for Space {SPACE_ID}'", temp_dir)
                run_git_cmd("git push origin main", temp_dir)
                print("Successfully pushed changes to Git.")
            else:
                print("No changes to commit.")
                
        except Exception as e:
            print(f"Git operation failed: {e}")
            raise

if __name__ == "__main__":
    main()
