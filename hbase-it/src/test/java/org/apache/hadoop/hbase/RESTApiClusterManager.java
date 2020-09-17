/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HBaseClusterManager.DEFAULT_RETRY_ATTEMPTS;
import static org.apache.hadoop.hbase.HBaseClusterManager.DEFAULT_RETRY_SLEEP_INTERVAL;
import static org.apache.hadoop.hbase.HBaseClusterManager.RETRY_ATTEMPTS_KEY;
import static org.apache.hadoop.hbase.HBaseClusterManager.RETRY_SLEEP_INTERVAL_KEY;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.ws.http.HTTPException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;
import org.apache.hbase.thirdparty.org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

/**
 * A ClusterManager implementation designed to control Cloudera Manager (http://www.cloudera.com)
 * clusters via REST API. This API uses HTTP GET requests against the cluster manager server to
 * retrieve information and POST/PUT requests to perform actions. As a simple example, to retrieve a
 * list of hosts from a CM server with login credentials admin:admin, a simple curl command would be
 *     curl -X POST -H "Content-Type:application/json" -u admin:admin \
 *         "http://this.is.my.server.com:7180/api/v8/hosts"
 *
 * This command would return a JSON result, which would need to be parsed to retrieve relevant
 * information. This action and many others are covered by this class.
 *
 * A note on nomenclature: while the ClusterManager interface uses a ServiceType enum when
 * referring to things like RegionServers and DataNodes, cluster managers often use different
 * terminology. As an example, Cloudera Manager (http://www.cloudera.com) would refer to a
 * RegionServer as a "role" of the HBase "service." It would further refer to "hbase" as the
 * "serviceType." Apache Ambari (http://ambari.apache.org) would call the RegionServer a
 * "component" of the HBase "service."
 *
 * This class will defer to the ClusterManager terminology in methods that it implements from
 * that interface, but uses Cloudera Manager's terminology when dealing with its API directly.
 *
 * DEBUG-level logging gives more details of the actions this class takes as they happen. Log at
 * TRACE-level to see the API requests and responses. TRACE-level logging on RetryCounter displays
 * wait times, so that can be helpful too.
 */
public class RESTApiClusterManager extends Configured implements ClusterManager {
  // Properties that need to be in the Configuration object to interact with the REST API cluster
  // manager. Most easily defined in hbase-site.xml, but can also be passed on the command line.
  private static final String REST_API_CLUSTER_MANAGER_HOSTNAME =
      "hbase.it.clustermanager.restapi.hostname";
  private static final String REST_API_CLUSTER_MANAGER_USERNAME =
      "hbase.it.clustermanager.restapi.username";
  private static final String REST_API_CLUSTER_MANAGER_PASSWORD =
      "hbase.it.clustermanager.restapi.password";
  private static final String REST_API_CLUSTER_MANAGER_CLUSTER_NAME =
      "hbase.it.clustermanager.restapi.clustername";
  private static final String REST_API_DELEGATE_CLUSTER_MANAGER =
    "hbase.it.clustermanager.restapi.delegate";

  private static final JsonParser parser = new JsonParser();

  // Some default values for the above properties.
  private static final String DEFAULT_SERVER_HOSTNAME = "http://localhost:7180";
  private static final String DEFAULT_SERVER_USERNAME = "admin";
  private static final String DEFAULT_SERVER_PASSWORD = "admin";
  private static final String DEFAULT_CLUSTER_NAME = "Cluster 1";

  // Fields for the hostname, username, password, and cluster name of the cluster management server
  // to be used.
  private String serverHostname;
  private String clusterName;

  // Each version of Cloudera Manager supports a particular API versions. Version 6 of this API
  // provides all the features needed by this class.
  private static final String API_VERSION = "v6";

  // Client instances are expensive, so use the same one for all our REST queries.
  private final Client client = ClientBuilder.newClient();

  // An instance of HBaseClusterManager is used for methods like the kill, resume, and suspend
  // because cluster managers don't tend to implement these operations.
  private ClusterManager hBaseClusterManager;

  private RetryCounterFactory retryCounterFactory;

  private static final Logger LOG = LoggerFactory.getLogger(RESTApiClusterManager.class);

  RESTApiClusterManager() { }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      // `Configured()` constructor calls `setConf(null)` before calling again with a real value.
      return;
    }

    final Class<? extends ClusterManager> clazz = conf.getClass(REST_API_DELEGATE_CLUSTER_MANAGER,
      HBaseClusterManager.class, ClusterManager.class);
    hBaseClusterManager = ReflectionUtils.newInstance(clazz, conf);

    serverHostname = conf.get(REST_API_CLUSTER_MANAGER_HOSTNAME, DEFAULT_SERVER_HOSTNAME);
    clusterName = conf.get(REST_API_CLUSTER_MANAGER_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);

    // Add filter to Client instance to enable server authentication.
    String serverUsername = conf.get(REST_API_CLUSTER_MANAGER_USERNAME, DEFAULT_SERVER_USERNAME);
    String serverPassword = conf.get(REST_API_CLUSTER_MANAGER_PASSWORD, DEFAULT_SERVER_PASSWORD);
    client.register(HttpAuthenticationFeature.basic(serverUsername, serverPassword));

    this.retryCounterFactory = new RetryCounterFactory(new RetryConfig()
      .setMaxAttempts(conf.getInt(RETRY_ATTEMPTS_KEY, DEFAULT_RETRY_ATTEMPTS))
      .setSleepInterval(conf.getLong(RETRY_SLEEP_INTERVAL_KEY, DEFAULT_RETRY_SLEEP_INTERVAL)));
  }

  @Override
  public void start(ServiceType service, String hostname, int port) {
    // With Cloudera Manager (6.3.x), sending a START command to a service role
    // that is already in the "Started" state is an error. CM will log a message
    // saying "Role must be stopped". It will complain similarly for other
    // expected state transitions.
    // A role process that has been `kill -9`'ed ends up with the service role
    // retaining the "Started" state but with the process marked as "unhealthy".
    // Instead of blindly issuing the START command, first send a STOP command
    // to ensure the START will be accepted.
    LOG.debug("Performing start of {} on {}:{}", service, hostname, port);
    final RoleState currentState = getRoleState(service, hostname);
    switch (currentState) {
      case NA:
      case BUSY:
      case UNKNOWN:
      case HISTORY_NOT_AVAILABLE:
        LOG.warn("Unexpected service state detected. Service START requested, but currently in"
          + " {} state. Attempting to start. {}, {}:{}", currentState, service, hostname, port);
        performClusterManagerCommand(service, hostname, RoleCommand.START);
        return;
      case STOPPING:
        LOG.warn("Unexpected service state detected. Service START requested, but currently in"
          + " {} state. Waiting for stop before attempting start. {}, {}:{}", currentState,
          service, hostname, port);
        waitFor(() -> Objects.equals(RoleState.STOPPED, getRoleState(service, hostname)));
        performClusterManagerCommand(service, hostname, RoleCommand.START);
        return;
      case STOPPED:
        performClusterManagerCommand(service, hostname, RoleCommand.START);
        return;
      case STARTING:
        LOG.warn("Unexpected service state detected. Service START requested, but already in"
          + " {} state. Ignoring current request and waiting for start to complete. {}, {}:{}",
          currentState, service, hostname, port);
        waitFor(()-> Objects.equals(RoleState.STARTED, getRoleState(service, hostname)));
        return;
      case STARTED:
        LOG.warn("Unexpected service state detected. Service START requested, but already in"
          + " {} state. Restarting. {}, {}:{}", currentState, service, hostname, port);
        performClusterManagerCommand(service, hostname, RoleCommand.RESTART);
        return;
    }
    throw new RuntimeException("should not happen.");
  }

  @Override
  public void stop(ServiceType service, String hostname, int port) {
    LOG.debug("Performing stop of {} on {}:{}", service, hostname, port);
    final RoleState currentState = getRoleState(service, hostname);
    switch (currentState) {
      case NA:
      case BUSY:
      case UNKNOWN:
      case HISTORY_NOT_AVAILABLE:
        LOG.warn("Unexpected service state detected. Service STOP requested, but already in"
          + " {} state. Attempting to stop. {}, {}:{}", currentState, service, hostname, port);
        performClusterManagerCommand(service, hostname, RoleCommand.STOP);
        return;
      case STOPPING:
        waitFor(() -> Objects.equals(RoleState.STOPPED, getRoleState(service, hostname)));
        return;
      case STOPPED:
        LOG.warn("Unexpected service state detected. Service STOP requested, but already in"
          + " {} state. Ignoring current request. {}, {}:{}", currentState, service, hostname,
          port);
        return;
      case STARTING:
        LOG.warn("Unexpected service state detected. Service STOP requested, but already in"
          + " {} state. Waiting for start to complete. {}, {}:{}", currentState, service, hostname,
          port);
        waitFor(()-> Objects.equals(RoleState.STARTED, getRoleState(service, hostname)));
        performClusterManagerCommand(service, hostname, RoleCommand.STOP);
        return;
      case STARTED:
        performClusterManagerCommand(service, hostname, RoleCommand.STOP);
        return;
    }
    throw new RuntimeException("should not happen.");
  }

  @Override
  public void restart(ServiceType service, String hostname, int port) {
    LOG.debug("Performing stop followed by start of {} on {}:{}", service, hostname, port);
    stop(service, hostname, port);
    start(service, hostname, port);
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname, int port) {
    LOG.debug("Issuing isRunning request against {} on {}:{}", service, hostname, port);
    return executeWithRetries(() -> {
      String serviceName = getServiceName(roleServiceType.get(service));
      String hostId = getHostId(hostname);
      RoleState roleState = getRoleState(serviceName, service.toString(), hostId);
      HealthSummary healthSummary = getHealthSummary(serviceName, service.toString(), hostId);
      return Objects.equals(RoleState.STARTED, roleState)
        && Objects.equals(HealthSummary.GOOD, healthSummary);
    });
  }

  @Override
  public void kill(ServiceType service, String hostname, int port) throws IOException {
    hBaseClusterManager.kill(service, hostname, port);
  }

  @Override
  public void suspend(ServiceType service, String hostname, int port) throws IOException {
    hBaseClusterManager.suspend(service, hostname, port);
  }

  @Override
  public void resume(ServiceType service, String hostname, int port) throws IOException {
    hBaseClusterManager.resume(service, hostname, port);
  }

  // Convenience method to execute command against role on hostname. Only graceful commands are
  // supported since cluster management APIs don't tend to let you SIGKILL things.
  private void performClusterManagerCommand(ServiceType role, String hostname,
    RoleCommand command) {
    // retry submitting the command until the submission succeeds.
    final long commandId = executeWithRetries(() -> {
      final String serviceName = getServiceName(roleServiceType.get(role));
      final String hostId = getHostId(hostname);
      final String roleName = getRoleName(serviceName, role.toString(), hostId);
      return doRoleCommand(serviceName, roleName, command);
    });
    LOG.debug("Command {} of {} on {} submitted as commandId {}",
      command, role, hostname, commandId);

    // assume the submitted command was asynchronous. wait on the commandId to be marked as
    // successful.
    waitFor(() -> hasCommandCompleted(commandId));
    if (!executeWithRetries(() -> hasCommandCompletedSuccessfully(commandId))) {
      final String msg = String.format("Command %s of %s on %s submitted as commandId %s failed.",
        command, role, hostname, commandId);
      // TODO: this does not interrupt the monkey. should it?
      throw new RuntimeException(msg);
    }
    LOG.debug("Command {} of {} on {} submitted as commandId {} completed successfully.",
      command, role, hostname, commandId);
  }

  /**
   * Issues a command (e.g. starting or stopping a role).
   * @return the commandId of a successfully submitted asynchronous command.
   */
  private long doRoleCommand(String serviceName, String roleName, RoleCommand roleCommand) {
    URI uri = UriBuilder.fromUri(serverHostname)
        .path("api")
        .path(API_VERSION)
        .path("clusters")
        .path(clusterName)
        .path("services")
        .path(serviceName)
        .path("roleCommands")
        .path(roleCommand.toString())
        .build();
    String body = "{ \"items\": [ \"" + roleName + "\" ] }";
    LOG.trace("Executing POST against {} with body {} ...", uri, body);
    WebTarget webTarget = client.target(uri);
    Invocation.Builder invocationBuilder =  webTarget.request(MediaType.APPLICATION_JSON);
    Response response = invocationBuilder.post(Entity.json(body));
    final int statusCode = response.getStatus();
    final String responseBody = response.readEntity(String.class);
    if (statusCode != Response.Status.OK.getStatusCode()) {
      LOG.warn(
        "RoleCommand failed with status code {} and response body {}", statusCode, responseBody);
      throw new HTTPException(statusCode);
    }

    LOG.trace("POST against {} completed with status code {} and response body {}",
      uri, statusCode, responseBody);
    return parser.parse(responseBody)
      .getAsJsonObject()
      .get("items")
      .getAsJsonArray()
      .get(0)
      .getAsJsonObject()
      .get("id")
      .getAsLong();
  }

  private HealthSummary getHealthSummary(String serviceName, String roleType, String hostId) {
    return HealthSummary.fromString(
      getRolePropertyValue(serviceName, roleType, hostId, "healthSummary"));
  }

  // This API uses a hostId to execute host-specific commands; get one from a hostname.
  private String getHostId(String hostname) {
    String hostId = null;
    URI uri = UriBuilder.fromUri(serverHostname)
      .path("api")
      .path(API_VERSION)
      .path("hosts")
      .build();
    JsonElement hosts = parser.parse(getFromURIGet(uri))
      .getAsJsonObject()
      .get("items");
    if (hosts != null) {
      // Iterate through the list of hosts, stopping once you've reached the requested hostname.
      for (JsonElement host : hosts.getAsJsonArray()) {
        if (host.getAsJsonObject().get("hostname").getAsString().equals(hostname)) {
          hostId = host.getAsJsonObject().get("hostId").getAsString();
          break;
        }
      }
    }

    return hostId;
  }

  private String getFromURIGet(URI uri) {
    LOG.trace("Executing GET against {} ...", uri);
    final Response response = client.target(uri)
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get();
    int statusCode = response.getStatus();
    final String responseBody = response.readEntity(String.class);
    if (statusCode != Response.Status.OK.getStatusCode()) {
      LOG.warn(
        "request failed with status code {} and response body {}", statusCode, responseBody);
      throw new HTTPException(statusCode);
    }
    // This API folds information as the value to an "items" attribute.
    LOG.trace("GET against {} completed with status code {} and response body {}",
      uri, statusCode, responseBody);
    return responseBody;
  }

  // This API assigns a unique role name to each host's instance of a role.
  private String getRoleName(String serviceName, String roleType, String hostId) {
    return getRolePropertyValue(serviceName, roleType, hostId, "name");
  }

  // Get the value of a property from a role on a particular host.
  private String getRolePropertyValue(String serviceName, String roleType, String hostId,
      String property) {
    String roleValue = null;
    URI uri = UriBuilder.fromUri(serverHostname)
      .path("api")
      .path(API_VERSION)
      .path("clusters")
      .path(clusterName)
      .path("services")
      .path(serviceName)
      .path("roles")
      .build();
    JsonElement roles = parser.parse(getFromURIGet(uri))
      .getAsJsonObject()
      .get("items");
    if (roles != null) {
      // Iterate through the list of roles, stopping once the requested one is found.
      for (JsonElement role : roles.getAsJsonArray()) {
        JsonObject roleObj = role.getAsJsonObject();
        if (roleObj.get("hostRef").getAsJsonObject().get("hostId").getAsString().equals(hostId) &&
          roleObj.get("type").getAsString().toLowerCase(Locale.ROOT)
            .equals(roleType.toLowerCase(Locale.ROOT))) {
          roleValue = roleObj.get(property).getAsString();
          break;
        }
      }
    }

    return roleValue;
  }

  private RoleState getRoleState(ServiceType service, String hostname) {
    return executeWithRetries(() -> {
      String serviceName = getServiceName(roleServiceType.get(service));
      String hostId = getHostId(hostname);
      RoleState state = getRoleState(serviceName, service.toString(), hostId);
      // sometimes the response (usually the first) is null. retry those.
      return Objects.requireNonNull(state);
    });
  }

  private RoleState getRoleState(String serviceName, String roleType, String hostId) {
    return RoleState.fromString(
      getRolePropertyValue(serviceName, roleType, hostId, "roleState"));
  }

  // Convert a service (e.g. "HBASE," "HDFS") into a service name (e.g. "HBASE-1," "HDFS-1").
  private String getServiceName(Service service) {
    String serviceName = null;
    URI uri = UriBuilder.fromUri(serverHostname).path("api").path(API_VERSION).path("clusters")
      .path(clusterName).path("services").build();
    JsonElement services = parser.parse(getFromURIGet(uri))
      .getAsJsonObject()
      .get("items");
    if (services != null) {
      // Iterate through the list of services, stopping once the requested one is found.
      for (JsonElement serviceEntry : services.getAsJsonArray()) {
        if (serviceEntry.getAsJsonObject().get("type").getAsString().equals(service.toString())) {
          serviceName = serviceEntry.getAsJsonObject().get("name").getAsString();
          break;
        }
      }
    }

    return serviceName;
  }

  private Optional<JsonObject> getCommand(final long commandId) {
    final URI uri = UriBuilder.fromUri(serverHostname)
      .path("api")
      .path(API_VERSION)
      .path("commands")
      .path(Long.toString(commandId))
      .build();
    return Optional.ofNullable(getFromURIGet(uri))
      .map(parser::parse)
      .map(JsonElement::getAsJsonObject);
  }

  /**
   * Return {@code true} if the {@code commandId} has finished processing.
   */
  private boolean hasCommandCompleted(final long commandId) {
    return getCommand(commandId)
      .map(val -> {
        final boolean isActive = val.get("active").getAsBoolean();
        if (isActive) {
          LOG.debug("command {} is still active.", commandId);
        }
        return !isActive;
      })
      .orElse(false);
  }

  /**
   * Return {@code true} if the {@code commandId} has finished successfully.
   */
  private boolean hasCommandCompletedSuccessfully(final long commandId) {
    return getCommand(commandId)
      .filter(val -> {
        final boolean isActive = val.get("active").getAsBoolean();
        if (isActive) {
          LOG.debug("command {} is still active.", commandId);
        }
        return !isActive;
      })
      .map(val -> {
        final boolean isSuccess = val.get("success").getAsBoolean();
        LOG.debug("command {} completed as {}.", commandId, isSuccess);
        return isSuccess;
      })
      .orElse(false);
  }

  /**
   * Helper method for executing retryable work.
   */
  private <T> T executeWithRetries(final Callable<T> callable) {
    final RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        if (retryCounter.shouldRetry()) {
          LOG.debug("execution failed with exception. Retrying.", e);
        } else {
          throw new RuntimeException("retries exhausted", e);
        }
      }
      try {
        retryCounter.sleepUntilNextRetry();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void waitFor(final Callable<Boolean> predicate) {
    final RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        if (Objects.equals(true, predicate.call())) {
          return;
        }
      } catch (Exception e) {
        if (retryCounter.shouldRetry()) {
          LOG.debug("execution failed with exception. Retrying.", e);
        } else {
          throw new RuntimeException("retries exhausted", e);
        }
      }
      try {
        retryCounter.sleepUntilNextRetry();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /*
   * Some enums to guard against bad calls.
   */

  // The RoleCommand enum is used by the doRoleCommand method to guard against non-existent methods
  // being invoked on a given role.
  // TODO: Integrate zookeeper and hdfs related failure injections (Ref: HBASE-14261).
  private enum RoleCommand {
    START, STOP, RESTART;

    // APIs tend to take commands in lowercase, so convert them to save the trouble later.
    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /**
   * Represents the configured run state of a role.
   * @see <a href="https://archive.cloudera.com/cm6/6.3.0/generic/jar/cm_api/apidocs/json_ApiRoleState.html">
   *   https://archive.cloudera.com/cm6/6.3.0/generic/jar/cm_api/apidocs/json_ApiRoleState.html</a>
   */
  private enum RoleState {
    HISTORY_NOT_AVAILABLE, UNKNOWN, STARTING, STARTED, BUSY, STOPPING, STOPPED, NA;

    public static RoleState fromString(final String value) {
      if (StringUtils.isBlank(value)) {
        return null;
      }
      return RoleState.valueOf(value.toUpperCase());
    }
  }

  /**
   * Represents of the high-level health status of a subject in the cluster.
   * @see <a href="https://archive.cloudera.com/cm6/6.3.0/generic/jar/cm_api/apidocs/json_ApiHealthSummary.html">
   *   https://archive.cloudera.com/cm6/6.3.0/generic/jar/cm_api/apidocs/json_ApiHealthSummary.html</a>
   */
  private enum HealthSummary {
    DISABLED, HISTORY_NOT_AVAILABLE, NOT_AVAILABLE, GOOD, CONCERNING, BAD;

    public static HealthSummary fromString(final String value) {
      if (StringUtils.isBlank(value)) {
        return null;
      }
      return HealthSummary.valueOf(value.toUpperCase());
    }
  }

  // ClusterManager methods take a "ServiceType" object (e.g. "HBASE_MASTER," "HADOOP_NAMENODE").
  // These "service types," which cluster managers call "roles" or "components," need to be mapped
  // to their corresponding service (e.g. "HBase," "HDFS") in order to be controlled.
  private static final Map<ServiceType, Service> roleServiceType = buildRoleServiceTypeMap();

  private static Map<ServiceType, Service> buildRoleServiceTypeMap() {
    final Map<ServiceType, Service> ret = new HashMap<>();
    ret.put(ServiceType.HADOOP_NAMENODE, Service.HDFS);
    ret.put(ServiceType.HADOOP_DATANODE, Service.HDFS);
    ret.put(ServiceType.HADOOP_JOBTRACKER, Service.MAPREDUCE);
    ret.put(ServiceType.HADOOP_TASKTRACKER, Service.MAPREDUCE);
    ret.put(ServiceType.HBASE_MASTER, Service.HBASE);
    ret.put(ServiceType.HBASE_REGIONSERVER, Service.HBASE);
    return Collections.unmodifiableMap(ret);
  }

  enum Service {
    HBASE, HDFS, MAPREDUCE
  }
}
