/**
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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

  // Some default values for the above properties.
  private static final String DEFAULT_SERVER_HOSTNAME = "http://localhost:7180";
  private static final String DEFAULT_SERVER_USERNAME = "admin";
  private static final String DEFAULT_SERVER_PASSWORD = "admin";
  private static final String DEFAULT_CLUSTER_NAME = "Cluster 1";

  // Fields for the hostname, username, password, and cluster name of the cluster management server
  // to be used.
  private String serverHostname;
  private String serverUsername;
  private String serverPassword;
  private String clusterName;

  // Each version of Cloudera Manager supports a particular API versions. Version 6 of this API
  // provides all the features needed by this class.
  private static final String API_VERSION = "v6";

  // Client instances are expensive, so use the same one for all our REST queries.
  private Client client = Client.create();

  // An instance of HBaseClusterManager is used for methods like the kill, resume, and suspend
  // because cluster managers don't tend to implement these operations.
  private ClusterManager hBaseClusterManager;

  private static final Log LOG = LogFactory.getLog(RESTApiClusterManager.class);

  RESTApiClusterManager() {
    hBaseClusterManager = ReflectionUtils.newInstance(HBaseClusterManager.class,
        new IntegrationTestingUtility().getConfiguration());
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      // Configured gets passed null before real conf. Why? I don't know.
      return;
    }
    serverHostname = conf.get(REST_API_CLUSTER_MANAGER_HOSTNAME, DEFAULT_SERVER_HOSTNAME);
    serverUsername = conf.get(REST_API_CLUSTER_MANAGER_USERNAME, DEFAULT_SERVER_USERNAME);
    serverPassword = conf.get(REST_API_CLUSTER_MANAGER_PASSWORD, DEFAULT_SERVER_PASSWORD);
    clusterName = conf.get(REST_API_CLUSTER_MANAGER_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);

    // Add filter to Client instance to enable server authentication.
    client.addFilter(new HTTPBasicAuthFilter(serverUsername, serverPassword));
  }

  @Override
  public void start(ServiceType service, String hostname) throws IOException {
    performClusterManagerCommand(service, hostname, RoleCommand.START);
  }

  @Override
  public void stop(ServiceType service, String hostname) throws IOException {
    performClusterManagerCommand(service, hostname, RoleCommand.STOP);
  }

  @Override
  public void restart(ServiceType service, String hostname) throws IOException {
    performClusterManagerCommand(service, hostname, RoleCommand.RESTART);
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname) throws IOException {
    String serviceName = getServiceName(roleServiceType.get(service));
    String hostId = getHostId(hostname);
    String roleState = getRoleState(serviceName, service.toString(), hostId);
    String healthSummary = getHealthSummary(serviceName, service.toString(), hostId);
    boolean isRunning = false;

    // Use Yoda condition to prevent NullPointerException. roleState will be null if the "service
    // type" does not exist on the specified hostname.
    if ("STARTED".equals(roleState) && "GOOD".equals(healthSummary)) {
      isRunning = true;
    }

    return isRunning;
  }

  @Override
  public void kill(ServiceType service, String hostname) throws IOException {
    hBaseClusterManager.kill(service, hostname);
  }

  @Override
  public void suspend(ServiceType service, String hostname) throws IOException {
    hBaseClusterManager.suspend(service, hostname);
  }

  @Override
  public void resume(ServiceType service, String hostname) throws IOException {
    hBaseClusterManager.resume(service, hostname);
  }


  // Convenience method to execute command against role on hostname. Only graceful commands are
  // supported since cluster management APIs don't tend to let you SIGKILL things.
  private void performClusterManagerCommand(ServiceType role, String hostname, RoleCommand command)
      throws IOException {
    LOG.info("Performing " + command + " command against " + role + " on " + hostname + "...");
    String serviceName = getServiceName(roleServiceType.get(role));
    String hostId = getHostId(hostname);
    String roleName = getRoleName(serviceName, role.toString(), hostId);
    doRoleCommand(serviceName, roleName, command);
  }

  // Performing a command (e.g. starting or stopping a role) requires a POST instead of a GET.
  private void doRoleCommand(String serviceName, String roleName, RoleCommand roleCommand) {
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
    LOG.info("Executing POST against " + uri + " with body " + body + "...");
    ClientResponse response = client.resource(uri)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, body);

    int statusCode = response.getStatus();
    if (statusCode != Response.Status.OK.getStatusCode()) {
      throw new HTTPException(statusCode);
    }
  }

  // Possible healthSummary values include "GOOD" and "BAD."
  private String getHealthSummary(String serviceName, String roleType, String hostId)
      throws IOException {
    return getRolePropertyValue(serviceName, roleType, hostId, "healthSummary");
  }

  // This API uses a hostId to execute host-specific commands; get one from a hostname.
  private String getHostId(String hostname) throws IOException {
    String hostId = null;

    URI uri = UriBuilder.fromUri(serverHostname)
        .path("api")
        .path(API_VERSION)
        .path("hosts")
        .build();
    JsonNode hosts = getJsonNodeFromURIGet(uri);
    if (hosts != null) {
      // Iterate through the list of hosts, stopping once you've reached the requested hostname.
      for (JsonNode host : hosts) {
        if (host.get("hostname").getTextValue().equals(hostname)) {
          hostId = host.get("hostId").getTextValue();
          break;
        }
      }
    } else {
      hostId = null;
    }

    return hostId;
  }

  // Execute GET against URI, returning a JsonNode object to be traversed.
  private JsonNode getJsonNodeFromURIGet(URI uri) throws IOException {
    LOG.info("Executing GET against " + uri + "...");
    ClientResponse response = client.resource(uri)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get(ClientResponse.class);

    int statusCode = response.getStatus();
    if (statusCode != Response.Status.OK.getStatusCode()) {
      throw new HTTPException(statusCode);
    }
    // This API folds information as the value to an "items" attribute.
    return new ObjectMapper().readTree(response.getEntity(String.class)).get("items");
  }

  // This API assigns a unique role name to each host's instance of a role.
  private String getRoleName(String serviceName, String roleType, String hostId)
      throws IOException {
    return getRolePropertyValue(serviceName, roleType, hostId, "name");
  }

  // Get the value of a  property from a role on a particular host.
  private String getRolePropertyValue(String serviceName, String roleType, String hostId,
      String property) throws IOException {
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
    JsonNode roles = getJsonNodeFromURIGet(uri);
    if (roles != null) {
      // Iterate through the list of roles, stopping once the requested one is found.
      for (JsonNode role : roles) {
        if (role.get("hostRef").get("hostId").getTextValue().equals(hostId) &&
            role.get("type")
                .getTextValue()
                .toLowerCase()
                .equals(roleType.toLowerCase())) {
          roleValue = role.get(property).getTextValue();
          break;
        }
      }
    }

    return roleValue;
  }

  // Possible roleState values include "STARTED" and "STOPPED."
  private String getRoleState(String serviceName, String roleType, String hostId)
      throws IOException {
    return getRolePropertyValue(serviceName, roleType, hostId, "roleState");
  }

  // Convert a service (e.g. "HBASE," "HDFS") into a service name (e.g. "HBASE-1," "HDFS-1").
  private String getServiceName(Service service) throws IOException {
    String serviceName = null;
    URI uri = UriBuilder.fromUri(serverHostname)
        .path("api")
        .path(API_VERSION)
        .path("clusters")
        .path(clusterName)
        .path("services")
        .build();
    JsonNode services = getJsonNodeFromURIGet(uri);
    if (services != null) {
      // Iterate through the list of services, stopping once the requested one is found.
      for (JsonNode serviceEntry : services) {
        if (serviceEntry.get("type").getTextValue().equals(service.toString())) {
          serviceName = serviceEntry.get("name").getTextValue();
          break;
        }
      }
    }

    return serviceName;
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
      return name().toLowerCase();
    }
  }

  // ClusterManager methods take a "ServiceType" object (e.g. "HBASE_MASTER," "HADOOP_NAMENODE").
  // These "service types," which cluster managers call "roles" or "components," need to be mapped
  // to their corresponding service (e.g. "HBase," "HDFS") in order to be controlled.
  private static Map<ServiceType, Service> roleServiceType = new HashMap<ServiceType, Service>();
  static {
    roleServiceType.put(ServiceType.HADOOP_NAMENODE, Service.HDFS);
    roleServiceType.put(ServiceType.HADOOP_DATANODE, Service.HDFS);
    roleServiceType.put(ServiceType.HADOOP_JOBTRACKER, Service.MAPREDUCE);
    roleServiceType.put(ServiceType.HADOOP_TASKTRACKER, Service.MAPREDUCE);
    roleServiceType.put(ServiceType.HBASE_MASTER, Service.HBASE);
    roleServiceType.put(ServiceType.HBASE_REGIONSERVER, Service.HBASE);
  }

  private enum Service {
    HBASE, HDFS, MAPREDUCE
  }
}
