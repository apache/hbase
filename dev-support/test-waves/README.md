# Test Wave Scripts

Large tests are partitioned into waves for parallel GHA execution.

## Scripts

- `fetch-wave-files.py` - Fetch wave assignments from GitHub Actions artifacts
- `generate-wave-assignments.py` - Generate wave files from Jenkins test runtime data

## Usage

```bash
# Fetch wave file (requires GITHUB_TOKEN)
export GITHUB_TOKEN=your_token
python fetch-wave-files.py 2 master

# Run tests with wave profile
mvn test -PrunLargeTests-wave2 -Dwave.data.dir=/tmp/hbase-wave-files/master
```

See `yetus-jdk17-hadoop3-check.yml` and `test-waves-aggregation.yml` for CI usage.
