# Healthcare Facility Finder - Deployment Guide

## Prerequisites

1. **Lakebase Instance Running**
   - Instance: `ep-misty-forest-d8hvkz5k.database.us-east-2.cloud.databricks.com`
   - Database: `databricks_postgres`
   - Data loaded: 10,023 facilities

2. **OAuth Token**
   - Get the token from the gold curation notebook (cell 9)
   - The token variable is: `token`
   - Copy the entire JWT string

## Deployment Steps

### Option 1: Deploy via Databricks CLI

```bash
# Set environment variable with your token
export DATABRICKS_TOKEN="<your-jwt-token-here>"

# Deploy the app
databricks apps deploy healthcare-facility-finder \
  --source-code-path /Workspace/Users/emdleb@gmail.com/Data-AVengers/app \
  --env DATABRICKS_TOKEN="$DATABRICKS_TOKEN"
```

### Option 2: Deploy via UI

1. Navigate to the Apps page in Databricks
2. Find "healthcare-facility-finder"
3. Click "Deploy"
4. Under "Environment Variables", add:
   - Key: `DATABRICKS_TOKEN`
   - Value: `<paste-jwt-token-from-notebook>`
5. Deploy

### Option 3: Use Databricks Secrets (Recommended for Production)

1. Create a secret scope:
   ```bash
   databricks secrets create-scope healthcare-app
   ```

2. Add the token as a secret:
   ```bash
   databricks secrets put-secret healthcare-app lakebase-token
   ```

3. Update `app.py` to read from secrets:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   token = w.dbutils.secrets.get("healthcare-app", "lakebase-token")
   ```

## Verifying Deployment

1. Check deployment status:
   ```bash
   databricks apps get healthcare-facility-finder
   ```

2. View logs:
   ```bash
   databricks apps logs healthcare-facility-finder
   ```

3. Access the app:
   - URL: https://healthcare-facility-finder-7474655748033941.aws.databricksapps.com

## Troubleshooting

### "Database token not configured"
- Ensure `DATABRICKS_TOKEN` environment variable is set
- Check token hasn't expired (tokens expire after ~1 hour)
- Regenerate token from notebook cell 9 if needed

### "Failed to connect to database"
- Verify Lakebase instance is running
- Check network connectivity
- Verify token is valid

### "App Not Available"
- Check deployment status
- Review app logs
- Ensure all dependencies are in requirements.txt

## App Features

✅ **Working:**
- Dashboard metrics (10,023 facilities, 37 states, 1,000+ cities)
- Search by facility name
- Filter by state
- Filter by city
- Results table display
- Interactive map for facilities with GPS coordinates

## Next Steps

- Add more advanced search (by facility type, operator type)
- Add distance-based search (find facilities near me)
- Implement pagination for large result sets
- Add facility detail pages
- Set up automated token refresh