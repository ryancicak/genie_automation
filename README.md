# Automation Scripts

This folder contains scripts for automating Genie Space management tasks using Databricks Jobs and Service Principals.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment Variables:**
    Create a `.env` file in the root directory (do not commit this file) with your Databricks credentials:
    ```
    DATABRICKS_HOST=https://your-workspace-url.cloud.databricks.com/
    DATABRICKS_TOKEN=dapi...
    ```

## Scripts

### `backup_genie_config.py`

This script fetches the current configuration of a Genie Space (including instructions, trusted assets, and example SQLs) and commits it to this Git repository. This allows you to track the evolution of your Genie Space configuration over time and revert changes if needed.

**Prerequisites:**

1.  **Service Principal Setup**: The Service Principal running the Job must have a Git Personal Access Token (PAT) registered. See `references/git_automation.md` for instructions.
2.  **Databricks Secret**: Store the Git PAT in a Databricks Secret Scope.
    *   Example: `databricks secrets create-scope --scope my-scope`
    *   Example: `databricks secrets put --scope my-scope --key git-pat`

**Job Configuration:**

1.  Create a new Databricks Job.
2.  Add a **Task** of type **Python Script**.
3.  **Source**: Git Provider (select this repository).
4.  **Path**: `genie_automation/backup_genie_config.py`
5.  **Cluster**: Select a cluster or use a Job Cluster (Serverless is also supported if using the right runtime).
6.  **Parameters**: Add the following parameters:
    *   `--space-id`: The ID of the Genie Space to backup (e.g., `01ef...`).
    *   `--secret-scope`: The name of the secret scope containing your Git PAT (e.g., `my-scope`).
    *   `--secret-key`: The key for the Git PAT (e.g., `git-pat`).
    *   `--git-username`: (Optional) The username for the Git commit (default: `genie-backup-bot`).
    *   `--git-email`: (Optional) The email for the Git commit (default: `bot@company.com`).

**Schedule:**

Set a schedule (e.g., daily) to automatically capture snapshots of your Genie Space configuration.
