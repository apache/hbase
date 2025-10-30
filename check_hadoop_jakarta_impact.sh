#!/usr/bin/env sh
# Find import lines that end with any class name from the hardcoded Jetty 12 migration change list.
# Usage: ./check_hadoop_jakarta_impact.sh /path/to/hbase

# TODO: Drop before merge

set -eu

if [ $# -ne 1 ]; then
  echo "Usage: $0 /path/to/hbase" >&2
  exit 1
fi

HBASE_ROOT="$1"
if [ ! -d "$HBASE_ROOT" ]; then
  echo "Error: '$HBASE_ROOT' is not a directory." >&2
  exit 2
fi

# --- Hardcoded change list (exactly the list you provided) ---
CHANGED_FILES_LIST=$(cat <<'EOF'
---
 LICENSE-binary                                | 66 +++++++-------
 .../ensure-jars-have-correct-contents.sh      |  1 +
 .../ensure-jars-have-correct-contents.sh      |  1 +
 .../hadoop-client-minicluster/pom.xml         | 42 ++++++++-
 .../hadoop/fs/obs/OBSBlockOutputStream.java   |  2 +-
 .../apache/hadoop/fs/obs/OBSInputStream.java  |  2 +-
 .../examples/RequestLoggerFilter.java         | 41 +++------
 .../authentication/examples/WhoServlet.java   |  8 +-
 hadoop-common-project/hadoop-auth/pom.xml     |  4 +-
 .../AltKerberosAuthenticationHandler.java     |  6 +-
 .../server/AuthenticationFilter.java          | 22 ++---
 .../server/AuthenticationHandler.java         |  6 +-
 .../server/AuthenticationToken.java           |  2 +-
 .../JWTRedirectAuthenticationHandler.java     |  8 +-
 .../server/KerberosAuthenticationHandler.java |  6 +-
 .../server/LdapAuthenticationHandler.java     |  6 +-
 .../MultiSchemeAuthenticationHandler.java     |  6 +-
 .../server/PseudoAuthenticationHandler.java   |  6 +-
 .../authentication/util/CertificateUtil.java  |  2 +-
 .../util/FileSignerSecretProvider.java        |  2 +-
 .../util/RolloverSignerSecretProvider.java    |  2 +-
 .../util/SignerSecretProvider.java            |  2 +-
 .../util/ZKSignerSecretProvider.java          |  2 +-
 hadoop-common-project/hadoop-common/pom.xml   |  8 +-
 .../org/apache/hadoop/conf/ConfServlet.java   | 10 +--
 .../hadoop/conf/ReconfigurationServlet.java   |  8 +-
 .../hadoop/http/AdminAuthorizedServlet.java   |  8 +-
 .../org/apache/hadoop/http/HttpServer2.java   | 88 ++++++++++---------
 .../hadoop/http/HttpServer2Metrics.java       | 56 ------------
 .../apache/hadoop/http/IsActiveServlet.java   |  6 +-
 .../org/apache/hadoop/http/NoCacheFilter.java | 14 +--
 .../hadoop/http/ProfileOutputServlet.java     |  8 +-
 .../apache/hadoop/http/ProfileServlet.java    |  6 +-
 .../hadoop/http/ProfilerDisabledServlet.java  |  6 +-
 .../apache/hadoop/http/PrometheusServlet.java |  8 +-
 .../org/apache/hadoop/http/WebServlet.java    |  8 +-
 .../hadoop/http/lib/StaticUserWebFilter.java  | 16 ++--
 .../org/apache/hadoop/jmx/JMXJsonServlet.java |  8 +-
 .../java/org/apache/hadoop/log/LogLevel.java  |  8 +-
 .../server/ProxyUserAuthenticationFilter.java | 12 +--
 .../security/http/CrossOriginFilter.java      | 16 ++--
 .../http/RestCsrfPreventionFilter.java        | 18 ++--
 .../security/http/XFrameOptionsFilter.java    | 16 ++--
 .../DelegationTokenAuthenticationFilter.java  | 14 +--
 .../DelegationTokenAuthenticationHandler.java |  8 +-
 ...eDelegationTokenAuthenticationHandler.java |  6 +-
 .../token/delegation/web/ServletUtils.java    |  2 +-
 .../hadoop/util/HttpExceptionUtils.java       |  6 +-
 .../org/apache/hadoop/util/ServletUtil.java   |  4 +-
 hadoop-common-project/hadoop-kms/pom.xml      |  4 +-
 .../hadoop/crypto/key/kms/server/KMS.java     | 26 +++---
 .../kms/server/KMSAuthenticationFilter.java   | 41 +++------
 .../key/kms/server/KMSExceptionsProvider.java |  6 +-
 .../crypto/key/kms/server/KMSJSONReader.java  | 12 +--
 .../crypto/key/kms/server/KMSJSONWriter.java  | 12 +--
 .../crypto/key/kms/server/KMSMDCFilter.java   | 14 +--
 .../crypto/key/kms/server/KMSWebApp.java      |  4 +-
 .../hadoop/hdfs/web/WebHdfsFileSystem.java    |  4 +-
 .../hdfs/web/resources/HttpOpParam.java       |  2 +-
 .../hadoop-hdfs-httpfs/pom.xml                |  4 +-
 .../hadoop/fs/http/client/HttpFSUtils.java    |  2 +-
 .../server/CheckUploadContentTypeFilter.java  | 16 ++--
 .../server/HttpFSAuthenticationFilter.java    |  4 +-
 .../http/server/HttpFSExceptionProvider.java  |  4 +-
 .../hadoop/fs/http/server/HttpFSServer.java   | 30 +++----
 .../lib/servlet/FileSystemReleaseFilter.java  | 12 +--
 .../hadoop/lib/servlet/HostnameFilter.java    | 12 +--
 .../apache/hadoop/lib/servlet/MDCFilter.java  | 14 +--
 .../hadoop/lib/servlet/ServerWebApp.java      |  4 +-
 .../hadoop/lib/wsrs/ExceptionProvider.java    |  4 +-
 .../hadoop/lib/wsrs/InputStreamEntity.java    |  2 +-
 .../hadoop/lib/wsrs/JSONMapProvider.java      | 12 +--
 .../apache/hadoop/lib/wsrs/JSONProvider.java  | 12 +--
 .../hadoop/lib/wsrs/ParametersProvider.java   |  2 +-
 .../metrics/NamenodeBeanMetrics.java          |  2 +-
 .../server/federation/metrics/RBFMetrics.java | 12 +--
 .../federation/router/ConnectionManager.java  |  2 +-
 .../federation/router/ConnectionPool.java     |  2 +-
 .../router/IsRouterActiveServlet.java         |  2 +-
 .../federation/router/RouterFsckServlet.java  |  6 +-
 .../federation/router/RouterHttpServer.java   |  2 +-
 .../router/RouterNetworkTopologyServlet.java  |  6 +-
 .../federation/router/RouterRpcClient.java    |  6 +-
 .../router/RouterWebHdfsMethods.java          | 10 +--
 .../server/GetJournalEditServlet.java         |  8 +-
 .../hdfs/qjournal/server/JournalNode.java     |  2 +-
 .../server/JournalNodeHttpServer.java         |  2 +-
 .../server/aliasmap/InMemoryAliasMap.java     |  4 +-
 .../HostRestrictingAuthorizationFilter.java   | 16 ++--
 .../hadoop/hdfs/server/common/JspHelper.java  |  4 +-
 .../hdfs/server/datanode/BlockScanner.java    |  6 +-
 .../hadoop/hdfs/server/datanode/DataNode.java |  8 +-
 .../datanode/web/DatanodeHttpServer.java      |  4 +-
 ...RestrictingAuthorizationFilterHandler.java |  4 +-
 .../web/RestCsrfPreventionFilterHandler.java  |  2 +-
 .../hdfs/server/namenode/DfsServlet.java      |  4 +-
 .../hdfs/server/namenode/FSNamesystem.java    | 22 ++---
 .../hdfs/server/namenode/FsckServlet.java     |  6 +-
 .../hdfs/server/namenode/ImageServlet.java    | 12 +--
 .../hdfs/server/namenode/NNStorage.java       |  2 +-
 .../server/namenode/NameNodeHttpServer.java   |  2 +-
 .../namenode/NetworkTopologyServlet.java      |  8 +-
 .../namenode/StartupProgressServlet.java      |  4 +-
 .../hdfs/server/namenode/TransferFsImage.java |  4 +-
 .../web/resources/NamenodeWebHdfsMethods.java | 40 ++++-----
 .../OfflineImageReconstructor.java            |  2 +-
 .../apache/hadoop/hdfs/web/AuthFilter.java    | 14 +--
 .../apache/hadoop/hdfs/web/ParamFilter.java   | 16 ++--
 .../hdfs/web/resources/ExceptionHandler.java  | 12 +--
 .../hdfs/web/resources/UserProvider.java      |  8 +-
 .../jobhistory/JobHistoryEventHandler.java    |  2 +-
 .../mapreduce/v2/app/JobEndNotifier.java      | 32 +++----
 .../mapreduce/v2/app/webapp/AMWebApp.java     |  2 +-
 .../v2/app/webapp/AMWebServices.java          | 34 +++----
 .../hadoop/mapreduce/v2/app/webapp/App.java   |  2 +-
 .../v2/app/webapp/AppController.java          |  2 +-
 .../v2/app/webapp/JAXBContextResolver.java    |  8 +-
 .../v2/app/webapp/dao/AMAttemptInfo.java      |  6 +-
 .../v2/app/webapp/dao/AMAttemptsInfo.java     |  8 +-
 .../mapreduce/v2/app/webapp/dao/AppInfo.java  |  6 +-
 .../app/webapp/dao/BlacklistedNodesInfo.java  |  6 +-
 .../v2/app/webapp/dao/ConfEntryInfo.java      |  6 +-
 .../mapreduce/v2/app/webapp/dao/ConfInfo.java |  6 +-
 .../v2/app/webapp/dao/CounterGroupInfo.java   |  8 +-
 .../v2/app/webapp/dao/CounterInfo.java        |  6 +-
 .../v2/app/webapp/dao/JobCounterInfo.java     |  8 +-
 .../mapreduce/v2/app/webapp/dao/JobInfo.java  |  8 +-
 .../webapp/dao/JobTaskAttemptCounterInfo.java |  8 +-
 .../app/webapp/dao/JobTaskAttemptState.java   |  6 +-
 .../v2/app/webapp/dao/JobTaskCounterInfo.java |  8 +-
 .../mapreduce/v2/app/webapp/dao/JobsInfo.java |  6 +-
 .../v2/app/webapp/dao/MapTaskAttemptInfo.java |  2 +-
 .../app/webapp/dao/ReduceTaskAttemptInfo.java |  2 +-
 .../v2/app/webapp/dao/TaskAttemptInfo.java    | 10 +--
 .../v2/app/webapp/dao/TaskAttemptsInfo.java   |  4 +-
 .../app/webapp/dao/TaskCounterGroupInfo.java  |  6 +-
 .../v2/app/webapp/dao/TaskCounterInfo.java    |  6 +-
 .../mapreduce/v2/app/webapp/dao/TaskInfo.java |  8 +-
 .../v2/app/webapp/dao/TasksInfo.java          |  6 +-
 .../ContainerLogsInfoMessageBodyReader.java   | 12 +--
 .../RemoteLogPathsMessageBodyReader.java      | 12 +--
 hadoop-project/pom.xml                        | 74 ++++++----------
 .../fs/azure/AzureNativeFileSystemStore.java  |  2 +-
 .../hadoop/fs/azure/MockStorageInterface.java |  4 +-
 .../service/ResourceEstimatorServer.java      |  2 +-
 .../service/ResourceEstimatorService.java     | 16 ++--
 .../api/records/timeline/TimelineAbout.java   |  8 +-
 .../TimelineDelegationTokenResponse.java      |  8 +-
 .../api/records/timeline/TimelineDomain.java  |  8 +-
 .../api/records/timeline/TimelineDomains.java |  8 +-
 .../records/timeline/TimelineEntities.java    |  8 +-
 .../api/records/timeline/TimelineEntity.java  |  8 +-
 .../api/records/timeline/TimelineEvent.java   |  8 +-
 .../api/records/timeline/TimelineEvents.java  |  8 +-
 .../api/records/timeline/TimelineHealth.java  |  8 +-
 .../records/timeline/TimelinePutResponse.java |  8 +-
 .../timeline/reader/TimelineDomainReader.java | 12 +--
 .../reader/TimelineEntitiesReader.java        | 12 +--
 .../reader/TimelinePutResponseReader.java     | 12 +--
 .../timeline/writer/TimelineDomainWriter.java | 12 +--
 .../writer/TimelineDomainsWriter.java         | 12 +--
 .../writer/TimelineEntitiesWriter.java        | 12 +--
 .../timeline/writer/TimelineEntityWriter.java | 12 +--
 .../timeline/writer/TimelineEventsWriter.java | 12 +--
 .../writer/TimelinePutResponseWriter.java     | 12 +--
 .../timelineservice/FlowActivityEntity.java   |  2 +-
 .../timelineservice/FlowRunEntity.java        |  2 +-
 .../timelineservice/TimelineDomain.java       |  8 +-
 .../timelineservice/TimelineEntities.java     |  8 +-
 .../timelineservice/TimelineEntity.java       |  8 +-
 .../timelineservice/TimelineEvent.java        |  8 +-
 .../timelineservice/TimelineMetric.java       |  8 +-
 .../TimelineWriteResponse.java                |  8 +-
 .../reader/TimelineDomainReader.java          | 12 +--
 .../reader/TimelineEntitiesReader.java        | 12 +--
 .../reader/TimelineEntityReader.java          | 12 +--
 .../writer/TimelineDomainWriter.java          | 12 +--
 .../writer/TimelineEntitiesWriter.java        | 12 +--
 .../writer/TimelineEntitySetWriter.java       | 12 +--
 .../writer/TimelineEntityWriter.java          | 12 +--
 .../writer/TimelineHealthWriter.java          | 12 +--
 .../pom.xml                                   | 16 +++-
 .../appcatalog/application/AppCatalog.java    |  4 +-
 .../application/AppCatalogInitializer.java    |  5 +-
 .../application/YarnServiceClient.java        |  6 +-
 .../controller/AppDetailsController.java      | 20 ++---
 .../controller/AppListController.java         | 20 ++---
 .../controller/AppStoreController.java        | 18 ++--
 .../hadoop-yarn-services-api/pom.xml          |  8 +-
 .../yarn/service/client/ApiServiceClient.java | 16 ++--
 .../hadoop/yarn/service/webapp/ApiServer.java | 12 +--
 .../yarn/service/webapp/ApiServerWebApp.java  |  4 +-
 .../hadoop/yarn/service/ServiceScheduler.java |  8 +-
 .../yarn/service/conf/RestApiConstants.java   |  2 +-
 .../hadoop/yarn/service/utils/HttpUtil.java   | 10 +--
 .../hadoop-yarn/hadoop-yarn-client/pom.xml    |  7 +-
 .../client/api/ContainerShellWebSocket.java   | 45 +++++-----
 .../yarn/client/api/impl/YarnClientImpl.java  |  3 +-
 .../hadoop/yarn/client/cli/LogsCLI.java       | 14 +--
 .../hadoop/yarn/client/cli/SchedConfCLI.java  | 20 ++---
 .../hadoop-yarn/hadoop-yarn-common/pom.xml    |  4 +-
 .../client/api/impl/DirectTimelineWriter.java |  2 +-
 .../api/impl/FileSystemTimelineWriter.java    |  2 +-
 .../client/api/impl/TimelineClientImpl.java   |  2 +-
 .../client/api/impl/TimelineConnector.java    |  6 +-
 .../api/impl/TimelineReaderClientImpl.java    | 16 ++--
 .../client/api/impl/TimelineV2ClientImpl.java | 14 +--
 .../yarn/client/api/impl/TimelineWriter.java  | 10 +--
 .../yarn/logaggregation/LogToolUtils.java     |  8 +-
 .../yarn/webapp/BadRequestException.java      |  4 +-
 .../hadoop/yarn/webapp/ConflictException.java |  4 +-
 .../apache/hadoop/yarn/webapp/Controller.java |  6 +-
 .../yarn/webapp/DefaultWrapperServlet.java    | 12 +--
 .../apache/hadoop/yarn/webapp/Dispatcher.java | 10 +--
 .../yarn/webapp/ForbiddenException.java       |  4 +-
 .../yarn/webapp/GenericExceptionHandler.java  | 22 ++---
 .../hadoop/yarn/webapp/NotFoundException.java |  4 +-
 .../yarn/webapp/RemoteExceptionData.java      |  6 +-
 .../org/apache/hadoop/yarn/webapp/View.java   |  8 +-
 .../org/apache/hadoop/yarn/webapp/WebApp.java |  2 +-
 .../apache/hadoop/yarn/webapp/WebApps.java    |  4 +-
 .../webapp/YarnJacksonJaxbJsonProvider.java   | 13 +--
 .../hadoop/yarn/webapp/dao/ConfInfo.java      |  6 +-
 .../yarn/webapp/dao/QueueConfigInfo.java      |  8 +-
 .../yarn/webapp/dao/SchedConfUpdateInfo.java  | 10 +--
 .../hadoop/yarn/webapp/util/WebAppUtils.java  |  4 +-
 .../yarn/webapp/util/WebServiceClient.java    |  4 +-
 .../yarn/webapp/util/YarnWebServiceUtils.java | 10 +--
 .../hadoop-yarn/hadoop-yarn-csi/pom.xml       |  4 +
 .../ApplicationHistoryServer.java             |  6 +-
 .../webapp/AHSWebApp.java                     |  2 +-
 .../webapp/AHSWebServices.java                | 28 +++---
 .../webapp/ContextFactory.java                |  4 +-
 .../webapp/JAXBContextResolver.java           |  8 +-
 .../timeline/webapp/TimelineWebServices.java  | 32 +++----
 .../reader/ContainerLogsInfoListReader.java   | 12 +--
 .../timeline/reader/TimelineAboutReader.java  | 12 +--
 .../timeline/reader/TimelineDomainReader.java | 12 +--
 .../reader/TimelineDomainsReader.java         | 12 +--
 .../reader/TimelineEntitiesReader.java        | 12 +--
 .../timeline/reader/TimelineEntityReader.java | 12 +--
 .../timeline/reader/TimelineEventsReader.java | 12 +--
 .../reader/TimelinePutResponseReader.java     | 12 +--
 .../security/http/RMAuthenticationFilter.java | 14 +--
 .../TimelineAuthenticationFilter.java         |  4 +-
 .../yarn/server/webapp/AppInfoProvider.java   |  2 +-
 .../hadoop/yarn/server/webapp/LogServlet.java | 14 +--
 .../yarn/server/webapp/LogWebService.java     | 32 +++----
 .../server/webapp/LogWebServiceUtils.java     |  8 +-
 .../yarn/server/webapp/WebServices.java       |  6 +-
 .../globalpolicygenerator/GPGUtils.java       | 12 +--
 .../webapp/GPGWebServices.java                | 14 +--
 .../hadoop-yarn-server-nodemanager/pom.xml    | 24 ++++-
 .../amrmproxy/FederationInterceptor.java      |  3 +-
 .../webapp/ContainerShellWebSocket.java       | 27 +++---
 .../ContainerShellWebSocketServlet.java       | 10 +--
 .../webapp/JAXBContextResolver.java           |  8 +-
 .../nodemanager/webapp/NMWebAppFilter.java    | 16 ++--
 .../nodemanager/webapp/NMWebServices.java     | 64 +++++++-------
 .../nodemanager/webapp/TerminalServlet.java   | 10 +--
 .../server/nodemanager/webapp/WebServer.java  |  2 +-
 .../yarn/server/resourcemanager/RMNMInfo.java |  2 +-
 .../resourcemanager/ResourceManager.java      |  4 +-
 .../webapp/FairSchedulerAppsBlock.java        |  2 +-
 .../webapp/JAXBContextResolver.java           | 12 +--
 .../resourcemanager/webapp/RMWebApp.java      |  2 +-
 .../webapp/RMWebAppFilter.java                | 16 ++--
 .../resourcemanager/webapp/RMWebAppUtil.java  |  2 +-
 .../webapp/RMWebServiceProtocol.java          |  4 +-
 .../resourcemanager/webapp/RMWebServices.java | 44 +++++-----
 .../yarn/server/resourcemanager/MockNM.java   |  6 +-
 ...MWebServicesCapacitySchedDefaultLabel.java | 10 +--
 ...WebServicesCapacitySchedDynamicConfig.java |  6 +-
 ...apacitySchedDynamicConfigAbsoluteMode.java |  6 +-
 ...sCapacitySchedDynamicConfigWeightMode.java |  6 +-
 ...pacitySchedDynamicConfigWeightModeDQC.java |  6 +-
 ...vicesCapacitySchedLegacyQueueCreation.java | 10 +--
 ...ySchedLegacyQueueCreationAbsoluteMode.java | 10 +--
 ...rvicesCapacitySchedulerConfigMutation.java | 12 +--
 ...WebServicesCapacitySchedulerMixedMode.java |  6 +-
 ...hedulerMixedModeAbsoluteAndPercentage.java |  6 +-
 ...xedModeAbsoluteAndPercentageAndWeight.java |  6 +-
 ...eAbsoluteAndPercentageAndWeightVector.java |  6 +-
 ...rMixedModeAbsoluteAndPercentageVector.java |  6 +-
 ...tySchedulerMixedModeAbsoluteAndWeight.java |  6 +-
 ...dulerMixedModeAbsoluteAndWeightVector.java |  6 +-
 ...SchedulerMixedModePercentageAndWeight.java |  6 +-
 ...lerMixedModePercentageAndWeightVector.java |  6 +-
 ...estRMWebServicesConfigurationMutation.java | 16 ++--
 ...vicesFairSchedulerCustomResourceTypes.java | 12 +--
 .../webapp/helper/BufferedClientResponse.java |  4 +-
 .../webapp/reader/AppStateReader.java         | 14 +--
 ...pplicationSubmissionContextInfoReader.java | 14 +--
 .../reader/LabelsToNodesInfoReader.java       | 14 +--
 .../webapp/reader/NodeLabelsInfoReader.java   | 14 +--
 .../webapp/reader/NodeToLabelsInfoReader.java | 14 +--
 .../reader/ResourceOptionInfoReader.java      | 14 +--
 ...pplicationSubmissionContextInfoWriter.java | 16 ++--
 .../writer/ResourceOptionInfoWriter.java      | 16 ++--
 .../writer/SchedConfUpdateInfoWriter.java     | 16 ++--
 .../hadoop/yarn/server/router/Router.java     |  4 +-
 .../yarn/server/router/webapp/AppsBlock.java  |  2 +-
 .../webapp/DefaultRequestInterceptorREST.java |  8 +-
 .../webapp/FederationInterceptorREST.java     | 12 +--
 .../router/webapp/MetricsOverviewTable.java   |  2 +-
 .../server/router/webapp/NodeLabelsBlock.java |  2 +-
 .../yarn/server/router/webapp/NodesBlock.java |  2 +-
 .../router/webapp/RESTRequestInterceptor.java |  4 +-
 .../server/router/webapp/RouterBlock.java     |  2 +-
 .../server/router/webapp/RouterWebApp.java    |  2 +-
 .../router/webapp/RouterWebServiceUtil.java   | 22 ++---
 .../router/webapp/RouterWebServices.java      | 38 ++++----
 .../webapp/cache/RouterAppInfoCacheKey.java   |  2 +-
 .../MockDefaultRequestInterceptorREST.java    | 12 +--
 .../webapp/MockRESTRequestInterceptor.java    |  8 +-
 .../PassThroughRESTRequestInterceptor.java    |  6 +----
 .../pom.xml                                   |  8 +-
 ...TimelineReaderWebServicesHBaseStorage.java |  8 +-
 .../TimelineCollectorWebService.java          | 36 ++++----
 .../reader/TimelineReaderWebServices.java     | 28 +++---
 .../TimelineReaderWebServicesUtils.java       |  2 +-
 ...ineReaderWhitelistAuthorizationFilter.java | 16 ++--
 ...ineReaderWhitelistAuthorizationFilter.java | 10 +--
 .../reader/TimelineAboutReader.java           | 12 +--
 .../reader/TimelineEntityReader.java          | 12 +--
 .../reader/TimelineEntitySetReader.java       | 12 +--
 .../reader/TimelineHealthReader.java          | 12 +--
 .../yarn/server/webproxy/ProxyUtils.java      |  8 +-
 .../server/webproxy/WebAppProxyServlet.java   | 16 ++--
 .../server/webproxy/amfilter/AmIpFilter.java  | 18 ++--
 .../amfilter/AmIpServletRequestWrapper.java   |  4 +-
EOF
)

# Create a temp file with unique class names extracted from the list
CLASSES_FILE="$(mktemp 2>/dev/null || echo /tmp/jetty12_classes.$$)"
echo "$CHANGED_FILES_LIST" \
  | grep -Eo '[^[:space:]]+\.java' \
  | awk '{
      n=$0; sub(/^.*\//,"",n); sub(/\.java$/,"",n); print n
    }' \
  | sort -u > "$CLASSES_FILE"

echo "Scanning HBase imports for $(wc -l < "$CLASSES_FILE" | tr -d ' ') changed classes..."

FOUND=0

# Portable search: find all *.java and grep them with regex
# Pattern logic:
#   ^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?    -> import or import static
#   [^;]*                                                   -> up to class token
#   [^A-Za-z0-9_$]ClassName[[:space:]]*;                    -> ensure ClassName is a full token before ';'
# Note: We avoid GNU grep --include by using find | xargs.
find "$HBASE_ROOT" -type f -name '*.java' -print0 | while IFS= read -r -d '' file; do
  :
done

# Iterate over each class and search
while IFS= read -r cls; do
  # Build ERE safely: prepend a non-identifier char before class name to approximate word boundary in imports.
  pattern="^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?[^;]*[^A-Za-z0-9_\$]${cls}[[:space:]]*;"
  # Use find+xargs for portability; suppress errors for long lines/binary files.
  hits=$(find "$HBASE_ROOT" -type f -name '*.java' -print0 \
    | xargs -0 grep -nEIH "$pattern" 2>/dev/null || true)
  if [ -n "$hits" ]; then
    echo "=== $cls ==="
    echo "$hits"
    FOUND=1
  fi
done < "$CLASSES_FILE"

if [ "$FOUND" -eq 0 ]; then
  echo "No matching imports found."
fi

# Cleanup temp file
rm -f "$CLASSES_FILE"
