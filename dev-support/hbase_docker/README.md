<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# hbase_docker

**Run HBase in Docker with one command.** Perfect for:
- 🎓 **Learning HBase** - Try it without installing anything
- 🔧 **HBase developers** - Test your changes in isolation
- ✅ **Release verification** - Validate release candidates
- 🧪 **Testing** - Run builds and tests in clean environments

---

## I Just Want to Try HBase (New Users Start Here!)

**Prerequisites:**
- Docker Desktop installed and running ([Get Docker](https://www.docker.com/products/docker-desktop))
- 5GB free disk space
- 15 minutes for first build

**Get HBase running in 3 commands:**

```bash
# 1. Go to the docker directory
cd dev-support/hbase_docker

# 2. Build HBase (one command, ~15 minutes)
./build-hbase.sh --tag master

# 3. Start HBase shell
docker run --platform linux/amd64 -it --rm hbase_local
```

**Try it out:**
```
# Inside HBase shell:
create 'test', 'cf'
put 'test', 'row1', 'cf:a', 'value1'
scan 'test'
exit
```

That's it! No installation, no configuration needed. 🎉

> **Apple Silicon (M1/M2/M3):** The build script automatically selects the right Dockerfile for your chip.
> For `docker run` commands, always pass `--platform linux/amd64` — the image is x86_64 running under Rosetta.

---

## I Want to Build from Different Sources

The script supports three modes:

### 1. From GitHub (Learning, Testing Branches)
```bash
./build-hbase.sh --tag master
./build-hbase.sh --tag branch-2.4
./build-hbase.sh --tag rel/2.6.7
```
**Use when:** Learning HBase, testing specific versions

### 2. From Downloaded Tarball (Release Verification)
```bash
./build-hbase.sh --tarball ~/Downloads/hbase-2.6.7-src.tar.gz
```
**Use when:** Verifying release candidates, testing official releases

### 3. From Local Source (Development)
```bash
# From any HBase repository
./build-hbase.sh --source ~/Code/hbase

# From current repository
./build-hbase.sh --source .
```
**Use when:** Testing your code changes, debugging, active development

**Perfect for HBase developers** working on patches or new features.

---

## Running HBase

After building, you have several options:

### Start HBase Shell (Interactive)
```bash
docker run --platform linux/amd64 -it --rm hbase_local
```
This starts HBase in standalone mode and gives you the HBase shell.

### Get Bash Shell (Explore Container)
```bash
docker run --platform linux/amd64 -it --rm hbase_local bash
```
Gives you a bash prompt to explore the container.

### Run Maven Commands (Build, Test)
```bash
docker run --platform linux/amd64 -it --rm hbase_local bash -c '
  cd /root/hbase && mvn clean install
'
```
The container has the full source code and Maven ready to use.

---

## Common Workflows

### I'm Learning HBase
```bash
# Build once
./build-hbase.sh --tag master

# Start HBase whenever you want to experiment
docker run --platform linux/amd64 -it --rm hbase_local

# Play around with HBase commands
create 'users', 'info', 'contact'
put 'users', 'john', 'info:name', 'John Doe'
get 'users', 'john'
scan 'users'
```

### I'm Verifying a Release Candidate
```bash
# Download RC tarball from Apache
wget https://dist.apache.org/repos/dist/dev/hbase/hbase-2.6.7RC0/hbase-2.6.7-src.tar.gz

# Build from it
cd dev-support/hbase_docker
./build-hbase.sh --tarball hbase-2.6.7-src.tar.gz hbase_267_rc0

# Test it works
docker run --platform linux/amd64 -it --rm hbase_267_rc0

# Run tests (optional)
docker run --platform linux/amd64 -it --rm hbase_267_rc0 bash -c '
  cd /root/hbase && mvn test
'
```

### I'm an HBase Developer Testing My Changes
```bash
# 1. Make changes to HBase code
vim hbase-server/src/main/java/org/apache/hadoop/hbase/MyClass.java

# 2. Build Docker image with your changes (includes uncommitted changes!)
cd dev-support/hbase_docker
./build-hbase.sh --source . my_feature

# 3. Test your changes in HBase shell
docker run --platform linux/amd64 -it --rm my_feature

# 4. Or run unit tests
docker run --platform linux/amd64 -it --rm my_feature bash -c '
  cd /root/hbase && mvn test -Dtest=MyTest
'

# 5. Run full test suite
docker run --platform linux/amd64 -it --rm my_feature bash -c '
  cd /root/hbase && mvn clean install
'
```

**Note:** This builds from your working directory, including uncommitted changes.
Great for testing patches before committing!

---

## Running Tests and Builds Inside Docker

The Docker image includes Maven and all dependencies. You can run any Maven command:

```bash
# Start bash in the container
docker run --platform linux/amd64 -it --rm hbase_local bash

# Inside container:
cd /root/hbase

# Run full build with tests
mvn clean install

# Run tests only
mvn test

# Skip tests
mvn clean install -DskipTests

# Run integration tests
mvn verify
```

**Important:** Files are **copied** into the image. Changes inside the container don't affect your local files.

### For Active Development: Volume Mount (Changes Persist!)

If you're actively developing and need changes to persist to your local files:

```bash
# 1. Build a base image once (use any stable branch)
./build-hbase.sh --tag master hbase_dev

# 2. Run with your local code mounted
cd /path/to/hbase
docker run --platform linux/amd64 \
  -v $(pwd):/workspace \
  -w /workspace \
  -it --rm hbase_dev bash

# 3. Inside container - changes persist to your local files!
mvn clean install        # Build from your local code
mvn spotless:apply       # Format your local files
mvn test                 # Run tests
exit

# 4. Changes are now in your local files
git diff                 # See the changes
git add .
```

**Why use this?**
- ✅ Code formatting (spotless) affects your local files
- ✅ Faster iteration (no rebuild needed)
- ✅ Generated files appear locally
- ✅ Perfect for active development sessions

**When not to use:**
- Just testing if something compiles → Use `--source` mode instead (simpler)

---

## Troubleshooting

### Docker daemon is not running
```
❌ ERROR: Docker daemon is not running
```
**Fix:** Start Docker Desktop app

### Out of disk space
```
⚠️ WARNING: Low disk space
```
**Fix:** Clean up old Docker images
```bash
docker image prune -a
```

### Build is very slow on Mac
This is normal on Apple Silicon — Docker uses Rosetta 2 emulation to run the x86_64 image.
- Expected on Apple Silicon: 15-20 minutes
- Native Linux (x86_64): 10-12 minutes

> **Note:** Apple is deprecating Rosetta. Once removed, builds will still work via QEMU emulation
> but will be slower. A native arm64 image is planned for a future release.

### Permission denied errors
Make sure the script is executable:
```bash
chmod +x build-hbase.sh
```

### Need help?
```bash
./build-hbase.sh --help
```

---

## Advanced Topics

<details>
<summary><b>Manual Docker Build Commands</b></summary>

If you prefer raw Docker commands instead of the script:

### Build from GitHub (default)
```bash
docker build --platform linux/amd64 -t hbase .
docker build --platform linux/amd64 --build-arg BRANCH_OR_TAG=branch-2.4 -t hbase .
```

### Build from tarball
```bash
cp /path/to/hbase-src.tar.gz hbase-src.tar.gz
docker build --platform linux/amd64 --build-arg INPUT_MODE=tarball -t hbase .
```

### Build from local source
```bash
# Use the source directory as the build context directly
docker build --platform linux/amd64 \
  --build-arg INPUT_MODE=source-dir \
  -f /path/to/hbase_docker/Dockerfile \
  -t hbase \
  /path/to/hbase
```

</details>

<details>
<summary><b>Technical Details: How It Works</b></summary>

### INPUT_MODE Build Argument

The Dockerfile supports three input modes via `INPUT_MODE`:
- `tag` (default) - Clone from GitHub
- `tarball` - Extract from local `hbase-src.tar.gz`
- `source-dir` - Copy from local `hbase/` directory

### What's Inside the Image

After build:
- HBase source code at `/root/hbase/`
- Built HBase binaries at `/root/hbase-bin/`
- Maven 3.8.6 at `/opt/maven/`
- Java 8 (Temurin JDK)
- All Maven dependencies cached

### Build Context Control

`.dockerignore` controls what gets copied:
- Excludes everything by default
- Includes only `hbase-src.tar.gz` or `hbase/` directory
- Keeps Docker context small and fast

</details>

<details>
<summary><b>Prerequisites Details</b></summary>

### Required
- **Docker installed and running**
  - macOS/Windows: [Docker Desktop](https://www.docker.com/products/docker-desktop)
  - Linux: [Docker Engine](https://docs.docker.com/engine/install/)
- **~5GB free disk space** (Docker image is ~2.7GB)

### Internet Connection
- **Required for `--tag` mode** (clone from GitHub)
- **Required for Maven dependencies** (all modes)
- **Not required** if Maven local repository is fully populated (rare)

The `build-hbase.sh` script checks these automatically and shows helpful errors.

</details>

<details>
<summary><b>Custom Image Names</b></summary>

Add a third argument to customize the image name:

```bash
./build-hbase.sh --tag branch-2.4 my_custom_name
./build-hbase.sh --tarball hbase.tar.gz hbase_rc_test
./build-hbase.sh --source ~/Code/hbase hbase_dev
```

Then run with your custom name:
```bash
docker run --platform linux/amd64 -it --rm my_custom_name
```

</details>

<details>
<summary><b>What the Script Does</b></summary>

The `build-hbase.sh` script automates setup:

1. **Validates prerequisites** - Docker running, disk space
2. **Cleans build context** - Removes conflicting files
3. **Sets up source** - Copies tarballs, validates paths
4. **Builds image** - Runs docker build with correct flags
5. **Reports results** - Shows next steps or troubleshooting

Logs are saved to `/tmp/hbase-docker-build.log`

You can skip the script and use raw Docker commands if you prefer.

</details>

---

## Quick Reference

```bash
# Build commands
./build-hbase.sh --tag branch-2.4              # From GitHub
./build-hbase.sh --tarball hbase.tar.gz        # From tarball
./build-hbase.sh --source ~/Code/hbase         # From local source

# Run commands
docker run --platform linux/amd64 -it --rm hbase_local              # HBase shell
docker run --platform linux/amd64 -it --rm hbase_local bash         # Bash shell

# Maven in container
cd /root/hbase && mvn clean install            # Full build with tests
cd /root/hbase && mvn test                     # Tests only

# Help
./build-hbase.sh --help                        # Show usage
docker images | grep hbase                     # List images
```
