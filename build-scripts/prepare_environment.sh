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

# We base the expected main branch and resulting maven version for clients on the hbase minor version
# The reason for this is hbase re-branches for each minor release (2.4, 2.5, 2.6, etc). At each re-branch
# the histories diverge. So we'll need to create our own fork of each new minor release branch.
# The convention is a fork named "hubspot-$minorVersion", and the maven coordinates "$minorVersion-hubspot-SNAPSHOT"
MINOR_VERSION="2.6"
MAIN_BRANCH="hubspot-${MINOR_VERSION}"

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
    SET_VERSION="${MINOR_VERSION}-hubspot-SNAPSHOT"
    REPO_NAME="AnyLinuxVersion_hs-hbase"
elif [[ "$GIT_BRANCH" != "hubspot" ]]; then
    SET_VERSION="${MINOR_VERSION}-${GIT_BRANCH}-SNAPSHOT"
    RELEASE="${RELEASE}~${GIT_BRANCH//[^[:alnum:]]/_}"
    REPO_NAME="AnyLinuxVersion_hs-hbase-develop"
else
    echo "Invalid git branch $GIT_BRANCH"
    exit 1
fi

RELEASE="${RELEASE}.${BUILD_NUMBER}"
FULL_BUILD_VERSION="${HBASE_VERSION}-${RELEASE}"

# SET_VERSION is not the most intuitive name, but it's required for set-maven-versions script
write-build-env-var SET_VERSION "$SET_VERSION"
write-build-env-var HBASE_VERSION "$HBASE_VERSION"
write-build-env-var PKG_RELEASE "$RELEASE"
write-build-env-var FULL_BUILD_VERSION "$FULL_BUILD_VERSION"
write-build-env-var REPO_NAME "$REPO_NAME"
# Adding this value as versioninfo.version ensures we have the same value as would normally
# show up in a non-hubspot hbase build. Otherwise due to set-maven-versions we'd end up
# with 2.6-hubspot-SNAPSHOT which is not very useful as a point of reference.
# Another option would be to pass in our FULL_BUILD_VERSION but that might cause some funniness
# with the expectations in VersionInfo.compareVersion().
write-build-env-var MAVEN_BUILD_ARGS "$MAVEN_BUILD_ARGS -Dversioninfo.version=$HBASE_VERSION"

echo "Building HBase version $HBASE_VERSION"
echo "Will deploy to nexus with version $SET_VERSION"
echo "Will create rpm with version $FULL_BUILD_VERSION"
echo "Will run maven with extra args $MAVEN_BUILD_ARGS"
