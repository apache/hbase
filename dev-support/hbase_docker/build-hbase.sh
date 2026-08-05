#!/usr/bin/env bash
set -e

# build-hbase.sh - Unified HBase Docker Build Script
#
# ONE script for all build modes. User specifies what they want.
#
# Usage:
#   ./build-hbase.sh --tag branch-2.4              # Clone from GitHub
#   ./build-hbase.sh --tarball hbase-2.6.7.tar.gz  # Build from tarball
#   ./build-hbase.sh --source /path/to/hbase       # Build from local dir
#   ./build-hbase.sh --source .                    # Build from current repo
#   ./build-hbase.sh --help                        # Show help

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="${LOG_FILE:-/tmp/hbase-docker-build.log}"
DEFAULT_IMAGE_NAME="hbase_local"

# On Apple Silicon (arm64), use m1/Dockerfile which is tuned for Rosetta emulation.
# Both cases build linux/amd64 images; m1/Dockerfile just handles arm64 host quirks.
HOST_ARCH=$(uname -m)
if [[ "$HOST_ARCH" == "arm64" || "$HOST_ARCH" == "aarch64" ]]; then
  DOCKERFILE="$SCRIPT_DIR/m1/Dockerfile"
else
  DOCKERFILE="$SCRIPT_DIR/Dockerfile"
fi
PLATFORM="linux/amd64"

#=============================================================================
# Prerequisites Check
#=============================================================================
check_prerequisites() {
  local errors=0

  # Check if Docker is installed
  if ! command -v docker &> /dev/null; then
    echo "❌ ERROR: Docker is not installed"
    echo
    echo "Please install Docker:"
    echo "  • Docker Desktop: https://www.docker.com/products/docker-desktop"
    echo "  • Linux: https://docs.docker.com/engine/install/"
    echo
    errors=1
  fi

  # Check if Docker daemon is running
  if command -v docker &> /dev/null; then
    if ! docker info &> /dev/null; then
      echo "❌ ERROR: Docker daemon is not running"
      echo
      echo "Please start Docker:"
      echo "  • macOS: Start Docker Desktop app"
      echo "  • Linux: sudo systemctl start docker"
      echo "  • Windows: Start Docker Desktop app"
      echo
      echo "Then verify with: docker ps"
      echo
      errors=1
    fi
  fi

  # Check disk space (warn if less than 5GB)
  if command -v df &> /dev/null; then
    local available_kb
    available_kb=$(df -k . | tail -1 | awk '{print $4}')
    local available_gb=$((available_kb / 1024 / 1024))

    if [ "$available_gb" -lt 5 ]; then
      echo "⚠️  WARNING: Low disk space (~${available_gb}GB available)"
      echo "   HBase Docker images require ~3GB"
      echo "   Consider cleaning up: docker image prune -a"
      echo
    fi
  fi

  return $errors
}

#=============================================================================
# Help
#=============================================================================
show_help() {
  cat <<'EOF'
╔════════════════════════════════════════╗
║   HBase Docker Build Script            ║
╚════════════════════════════════════════╝

USAGE:
  build-hbase.sh MODE VALUE [IMAGE_NAME]

MODES:
  --tag BRANCH_OR_TAG       Clone from GitHub
  --tarball FILE            Build from tarball
  --source PATH             Build from local directory

EXAMPLES:
  # Clone from GitHub branch
  ./build-hbase.sh --tag branch-2.4

  # Clone from GitHub tag
  ./build-hbase.sh --tag rel/2.6.7

  # Build from downloaded tarball
  ./build-hbase.sh --tarball ~/Downloads/hbase-2.6.7-src.tar.gz

  # Build from local HBase directory
  ./build-hbase.sh --source ~/Code/hbase

  # Build from current repository
  ./build-hbase.sh --source .

  # Custom image name
  ./build-hbase.sh --tag branch-2.4 my_hbase_image

OPTIONS:
  --help, -h                Show this help

ENVIRONMENT:
  LOG_FILE                  Path to build log (default: /tmp/hbase-docker-build.log)

EOF
}

#=============================================================================
# Build Functions
#=============================================================================

build_from_tag() {
  local branch_or_tag="$1"
  local image_name="$2"

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "MODE: Clone from GitHub"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Branch/Tag:  $branch_or_tag"
  echo "Image name:  $image_name"
  echo "Log file:    $LOG_FILE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo

  # Build
  echo "🚀 Starting Docker build (10-15 minutes)..."
  echo "   Platform:    $PLATFORM"
  echo "   Follow progress: tail -f $LOG_FILE"
  echo

  docker build --platform "$PLATFORM" \
    -f "$DOCKERFILE" \
    --build-arg INPUT_MODE=tag \
    --build-arg BRANCH_OR_TAG="$branch_or_tag" \
    -t "$image_name" \
    "$SCRIPT_DIR" 2>&1 | tee "$LOG_FILE"

  return ${PIPESTATUS[0]}
}

build_from_tarball() {
  local tarball_path="$1"
  local image_name="$2"

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "MODE: Build from Tarball"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Tarball:     $tarball_path"
  echo "Image name:  $image_name"
  echo "Log file:    $LOG_FILE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo

  # Verify tarball exists
  if [ ! -f "$tarball_path" ]; then
    echo "❌ ERROR: Tarball not found: $tarball_path"
    exit 1
  fi

  # Copy tarball
  echo "📦 Copying tarball to build context..."
  cp "$tarball_path" "$SCRIPT_DIR/hbase-src.tar.gz"
  echo "   Size: $(du -h "$SCRIPT_DIR/hbase-src.tar.gz" | cut -f1)"

  # Build
  echo "🚀 Starting Docker build (10-15 minutes)..."
  echo "   Platform:    $PLATFORM"
  echo "   Follow progress: tail -f $LOG_FILE"
  echo

  docker build --platform "$PLATFORM" \
    -f "$DOCKERFILE" \
    --build-arg INPUT_MODE=tarball \
    -t "$image_name" \
    "$SCRIPT_DIR" 2>&1 | tee "$LOG_FILE"

  local status=${PIPESTATUS[0]}

  # Clean up
  rm -f "$SCRIPT_DIR/hbase-src.tar.gz"

  return $status
}

build_from_source() {
  local source_path="$1"
  local image_name="$2"

  # Validate and resolve to absolute path
  if [ ! -d "$source_path" ]; then
    echo "❌ ERROR: Directory not found: $source_path"
    exit 1
  fi
  source_path="$(cd "$source_path" && pwd)"

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "MODE: Build from Local Source"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Source:      $source_path"
  echo "Image name:  $image_name"
  echo "Log file:    $LOG_FILE"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo

  # Verify it's an HBase repo
  if [ ! -f "$source_path/pom.xml" ]; then
    echo "❌ ERROR: Not an HBase repository (no pom.xml found)"
    echo "   Path: $source_path"
    exit 1
  fi

  echo "📁 Using source directory as build context..."
  echo "   This includes all files (including uncommitted changes)"

  # Temporarily copy the Dockerfile into the source tree so Docker can find it
  # while the source directory serves as the build context
  local temp_dockerfile="$source_path/.hbase-docker-temp"
  mkdir -p "$temp_dockerfile"
  cp "$DOCKERFILE" "$temp_dockerfile/Dockerfile"

  # .dockerignore must be at the build context root to take effect
  cat > "$source_path/.dockerignore" <<'DOCKERIGNORE'
# Exclude build artifacts and metadata from the Docker build context
target/
.git/
.idea/
*.iml
.hbase-docker-temp/
DOCKERIGNORE

  # Build from source directory
  echo "🚀 Starting Docker build (10-15 minutes)..."
  echo "   Platform:    $PLATFORM"
  echo "   Follow progress: tail -f $LOG_FILE"
  echo

  docker build --platform "$PLATFORM" \
    --build-arg INPUT_MODE=source-dir \
    -f "$temp_dockerfile/Dockerfile" \
    -t "$image_name" \
    "$source_path" 2>&1 | tee "$LOG_FILE"

  local status=${PIPESTATUS[0]}

  # Clean up temp files
  rm -rf "$temp_dockerfile"
  rm -f "$source_path/.dockerignore"

  return $status
}

show_success() {
  local image_name="$1"

  echo
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "✅ BUILD SUCCESSFUL"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Get image info
  docker images "$image_name" --format "Image: {{.Repository}}:{{.Tag}}\nSize:  {{.Size}}\nBuilt: {{.CreatedSince}}"

  echo
  echo "🚀 NEXT STEPS:"
  echo
  echo "1️⃣  Start HBase Shell (interactive):"
  echo "   docker run --platform $PLATFORM -it --rm $image_name"
  echo
  echo "2️⃣  Get Bash Shell (explore container):"
  echo "   docker run --platform $PLATFORM -it --rm $image_name bash"
  echo
  echo "3️⃣  Check HBase Version:"
  echo "   echo 'version' | docker run --platform $PLATFORM -i --rm $image_name"
  echo
  echo "4️⃣  View Build Log:"
  echo "   cat $LOG_FILE"
  echo
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

show_failure() {
  echo
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "❌ BUILD FAILED"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo
  echo "📋 Check the log file:"
  echo "   tail -100 $LOG_FILE"
  echo
  echo "🔍 Common issues:"
  echo "   • Network issues (git clone failed)"
  echo "   • Maven build errors"
  echo "   • Out of disk space"
  echo "   • Docker daemon issues"
  echo
}

#=============================================================================
# Main
#=============================================================================

main() {
  local mode=""
  local value=""
  local image_name="$DEFAULT_IMAGE_NAME"

  # Parse arguments
  case "${1:-}" in
    --help|-h)
      show_help
      exit 0
      ;;
    --tag)
      mode="tag"
      value="$2"
      image_name="${3:-$DEFAULT_IMAGE_NAME}"
      ;;
    --tarball)
      mode="tarball"
      value="$2"
      image_name="${3:-$DEFAULT_IMAGE_NAME}"
      ;;
    --source)
      mode="source"
      value="$2"
      image_name="${3:-$DEFAULT_IMAGE_NAME}"
      ;;
    "")
      echo "❌ ERROR: No mode specified"
      echo
      show_help
      exit 1
      ;;
    *)
      echo "❌ ERROR: Unknown mode: $1"
      echo
      show_help
      exit 1
      ;;
  esac

  # Validate value provided
  if [ -z "$value" ]; then
    echo "❌ ERROR: No value provided for --$mode"
    echo
    show_help
    exit 1
  fi

  echo
  echo "╔════════════════════════════════════════╗"
  echo "║   HBase Docker Build Orchestrator      ║"
  echo "╚════════════════════════════════════════╝"
  echo

  # Check prerequisites
  if ! check_prerequisites; then
    exit 1
  fi

  # Execute based on mode (use || to capture failure even under set -e)
  local build_status=0
  case "$mode" in
    tag)
      build_from_tag "$value" "$image_name" || build_status=$?
      ;;
    tarball)
      build_from_tarball "$value" "$image_name" || build_status=$?
      ;;
    source)
      build_from_source "$value" "$image_name" || build_status=$?
      ;;
  esac

  # Report results
  if [ $build_status -eq 0 ]; then
    show_success "$image_name"
  else
    show_failure
    exit 1
  fi
}

main "$@"
