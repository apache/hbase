#
# Generates the appropriate environment vars so that we:
# - build against the right version of hadoop, and properly set up maven
# - generate the correct maven version based on the branches
# - upload RPMs with the correct release based on the branch, and to the right yum repo
#
# Since we need to distribute .blazar.yaml to all sub-modules of the project, we define our constants once
# in this script which can be re-used by every .blazar.yaml.
#
set -ex
printenv

if [ $1 = "cdh5" ]; then
  HADOOP_DEP_VERSION="2.6.0-cdh5.16.2"
  VERSION_ARGS="-Phadoop-2.0 -Dhadoop-two.version=$HADOOP_DEP_VERSION"
else
  HADOOP_DEP_VERSION="3.3.1"
  VERSION_ARGS="-Phadoop-3.0 -Dhadoop.profile=3.0 -Dhadoop-three.version=$HADOOP_DEP_VERSION"
fi

# We base the expected main branch and resulting maven version for clients on the hbase minor version
# The reason for this is hbase re-branches for each minor release (2.4, 2.5, 2.6, etc). At each re-branch
# the histories diverge. So we'll need to create our own fork of each new minor release branch.
# The convention is a fork named "hubspot-$minorVersion", and the maven coordinates "$minorVersion-hubspot-SNAPSHOT"
MINOR_VERSION="2.5"
MAIN_BRANCH="hubspot-${MINOR_VERSION}"
MAIN_YUM_REPO="6_hs-hbase"
DEVELOP_YUM_REPO="6_hs-hbase-develop"

# If we bump our hadoop build version, we should bump this as well
# At some point it would be good to more closely link this to our hadoop build, but that can only happen
# once we update our apache-hadoop build to do a full maven. At which point we can probably change this to
# like 3.0-hubspot-SNAPSHOT and leave it at that.
MAVEN_ARGS="$VERSION_ARGS -Dgpg.skip=true -DskipTests=true"

#
# Validate inputs from blazar
#

if [ -z "$WORKSPACE" ]; then
    echo "Missing env var \$WORKSPACE"
    exit 1
fi
if [ -z "$GIT_BRANCH" ]; then
    echo "Missing env var \$GIT_BRANCH"
    exit 1
fi
if [ -z "$BUILD_COMMAND_RC_FILE" ]; then
    echo "Missing env var \$BUILD_COMMAND_RC_FILE"
    exit 1
fi

#
# Extract current hbase version from root pom.xml
#

# the pom.xml has an invalid xml namespace, so just remove that so xmllint can parse it.
cat $WORKSPACE/pom.xml | sed '2 s/xmlns=".*"//g' > pom.xml.tmp
HBASE_VERSION=$(echo "cat /project/properties/revision/text()" | xmllint --nocdata --shell pom.xml.tmp | sed '1d;$d')
rm pom.xml.tmp

# sanity check that we've got some that looks right. it wouldn't be the end of the world if we got it wrong, but
# will help avoid confusion.
if [[ ! "$HBASE_VERSION" =~ 2\.[0-9]+\.[0-9]+ ]]; then
    echo "Unexpected HBASE_Version extracted from pom.xml. Got $HBASE_VERSION but expected a string like '2.4.3', with 3 numbers separated by decimals, the first number being 2."
    exit 1
fi

#
# Generate branch-specific env vars
# We are going to generate the maven version and the RPM release here:
# - For the maven version, we need to special case our main branch
# - For RPM, we want our final version to be:
#   main branch: {hbase_version}-hs.{build_number}.el6
#   other branches: {hbase_version}-hs~{branch_name}.{build_number}.el6, where branch_name substitutes underscore for non-alpha-numeric characters
#

echo "Git branch $GIT_BRANCH. Detecting appropriate version override and RPM release."

RELEASE="hs"

if [[ "$GIT_BRANCH" = "$MAIN_BRANCH" ]]; then
    MAVEN_VERSION="${MINOR_VERSION}-hubspot-SNAPSHOT"
    YUM_REPO=$MAIN_YUM_REPO
elif [[ "$GIT_BRANCH" != "hubspot" ]]; then
    MAVEN_VERSION="${MINOR_VERSION}-${GIT_BRANCH}-SNAPSHOT"
    RELEASE="${RELEASE}~${GIT_BRANCH//[^[:alnum:]]/_}"
    YUM_REPO=$DEVELOP_YUM_REPO
else
    echo "Invalid git branch $GIT_BRANCH"
    exit 1
fi

RELEASE="${RELEASE}.${BUILD_NUMBER}"
FULL_BUILD_VERSION="${HBASE_VERSION}-${RELEASE}"

# Add into MAVEN_ARGS because we added this property in hbase-common/pom.xml so we
# could accurately reflect the full build version in the UI and elsewhere.
MAVEN_ARGS="$MAVEN_ARGS -Dhubspot.build.version=$HBASE_VERSION"

#
# Dump generated env vars into rc file
#

cat >> "$BUILD_COMMAND_RC_FILE" <<EOF
export MAVEN_ARGS='$MAVEN_ARGS'
export SET_VERSION='$MAVEN_VERSION'
export HBASE_VERSION='$HBASE_VERSION'
export PKG_RELEASE='$RELEASE'
export YUM_REPO='$YUM_REPO'
export FULL_BUILD_VERSION='$FULL_BUILD_VERSION'
EOF

echo "Building HBase version $HBASE_VERSION"
echo "Will use maven version $MAVEN_VERSION"
echo "Will run maven with extra args $MAVEN_ARGS"
echo "Will upload RPMs with version $FULL_BUILD_VERSION to $YUM_REPO"
