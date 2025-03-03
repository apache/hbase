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

import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.util.ClientBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster manager for K8s context. Supports taking destructive actions, and checking for the
 * presence of a running process. Assumes services are running behind some type of `Deployment` that
 * will handle starting replacement processes after a destructive action. Requires that the
 * configuration specify a Kubernetes namespace.
 * </p>
 * Note that the k8s java client is a bit dodgy unable to read responses when we call delete (i.e.
 * kill) and sometimes when checking isRunning; makes for noisy logs and sometimes the operations
 * overrun each other but generally succeed.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PodKillingK8sClusterManager extends Configured implements ClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(PodKillingK8sClusterManager.class);

  private static final String NAMESPACE_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".namespace";
  private static final String DEFAULT_NAMESPACE = "default";
  private static final String ZOOKEEPER_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".zookeeper_role";
  private static final String NAMENODE_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".namenode_role";
  private static final String JOURNALNODE_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".journalnode_role";
  private static final String DATANODE_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".datanode_role";
  private static final String MASTER_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".master_role";
  private static final String REGIONSERVER_ROLE_SELECTOR_CONF_KEY =
    PodKillingK8sClusterManager.class.getCanonicalName() + ".regionserver_role";
  private static final Set<ServiceType> SUPPORTED_SERVICE_TYPES = buildSupportedServiceTypesSet();

  private CoreV1Api api;
  private String namespace;

  private static Set<ServiceType> buildSupportedServiceTypesSet() {
    final Set<ServiceType> set = new HashSet<>();
    set.add(ServiceType.ZOOKEEPER_SERVER);
    set.add(ServiceType.HADOOP_NAMENODE);
    set.add(ServiceType.HADOOP_JOURNALNODE);
    set.add(ServiceType.HADOOP_DATANODE);
    set.add(ServiceType.HBASE_MASTER);
    set.add(ServiceType.HBASE_REGIONSERVER);
    return Collections.unmodifiableSet(set);
  }

  @Override
  public void setConf(Configuration configuration) {
    // This is usually called from the constructor (The call to super goes to
    // Configured which does a setConf in its construction) but the configuration
    // null at that time. After construction, setConf is called again.
    // Assume single-thread doing ClusterManager setup.
    if (configuration == null) {
      LOG.debug("Skipping because provided configuration=null");
      return;
    }
    if (getConf() != null) {
      LOG.debug("Skipping because configuration already set, getConf={}", getConf());
      return;
    }
    super.setConf(configuration);
    namespace = configuration.get(NAMESPACE_CONF_KEY, DEFAULT_NAMESPACE);
    LOG.info(
      "Configuration={}, namespace={}, hbase.rootdir={}, hbase.zookeeper.quorum={}, "
        + "hbase.client.zookeeper.quorum={}",
      configuration, namespace, configuration.get("hbase.rootdir"),
      configuration.get("hbase.zookeeper.quorum"),
      configuration.get("hbase.client.zookeeper.quorum"));
    final CoreV1Api coreV1Api;
    try {
      ApiClient client = ClientBuilder.cluster().build();
      io.kubernetes.client.openapi.Configuration.setDefaultApiClient(client);
      coreV1Api = new CoreV1Api();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed Kubernetes ApiClient construction", ioe);
    }
    api = coreV1Api;
  }

  @Override
  public void start(ServiceType service, String hostname, int port) {
    assertSupportedServiceType(service);
    // presumably kubernetes is automatically starting pods, so nothing to do here.
  }

  @Override
  public void stop(ServiceType service, String hostname, int port) throws IOException {
    assertSupportedServiceType(service);
    kill(hostname);
  }

  @Override
  public void restart(ServiceType service, String hostname, int port) throws IOException {
    assertSupportedServiceType(service);
    kill(hostname);
  }

  @Override
  public void kill(ServiceType service, String hostname, int port) throws IOException {
    assertSupportedServiceType(service);
    kill(hostname);
  }

  @Override
  public void suspend(ServiceType service, String hostname, int port) {
    throw unsupportedActionType("suspend");
  }

  @Override
  public void resume(ServiceType service, String hostname, int port) {
    throw unsupportedActionType("resume");
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname, int port) throws IOException {
    final String roleSelector = roleSelectorForServiceType(service);
    return isRunning(roleSelector, hostname);
  }

  private String roleSelectorForServiceType(final ServiceType serviceType) {
    assertSupportedServiceType(serviceType);
    switch (serviceType) {
      case HADOOP_NAMENODE:
        return getConf().get(NAMENODE_ROLE_SELECTOR_CONF_KEY, "namenode");
      case HADOOP_DATANODE:
        return getConf().get(DATANODE_ROLE_SELECTOR_CONF_KEY, "datanode");
      case HADOOP_JOURNALNODE:
        return getConf().get(JOURNALNODE_ROLE_SELECTOR_CONF_KEY, "journalnode");
      case ZOOKEEPER_SERVER:
        return getConf().get(ZOOKEEPER_ROLE_SELECTOR_CONF_KEY, "zookeeper");
      case HBASE_MASTER:
        return getConf().get(MASTER_ROLE_SELECTOR_CONF_KEY, "master");
      case HBASE_REGIONSERVER:
        return getConf().get(REGIONSERVER_ROLE_SELECTOR_CONF_KEY, "regionserver");
      default:
        throw new RuntimeException("should not happen");
    }
  }

  private boolean isRunning(String roleSelector, String hostname) throws IOException {
    if (api == null) {
      return false;
    }
    final V1PodList list;
    try {
      list = api.listNamespacedPod(namespace, null, null, null, null,
        "role in (" + roleSelector + ")", null, null, null, null, null);
    } catch (ApiException e) {
      throw convertApiException(e);
    }
    if (list == null) {
      return false;
    }
    for (V1Pod item : list.getItems()) {
      final String podName =
        Optional.ofNullable(item).map(V1Pod::getMetadata).map(V1ObjectMeta::getName).orElse(null);
      if (StringUtils.isEmpty(podName)) {
        LOG.warn("Listing of namespace '{}' contains entry with empty pod name.", namespace);
        continue;
      }
      final String status =
        Optional.of(item).map(V1Pod::getStatus).map(V1PodStatus::getPhase).orElse(null);
      if (status == null) {
        LOG.warn("Status of pod {}.{} not returned by the API.", namespace, podName);
        return false;
      }
      if (hostname.startsWith(podName) && "running".equalsIgnoreCase(status)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param hostname fully qualified hostname of the target pod.
   * @return The pod name without the domain.
   */
  private static String hostNameToPodName(String hostname) {
    return hostname.substring(0, hostname.indexOf("."));
  }

  private void kill(String hostname) throws IOException {
    try {
      final String podName = hostNameToPodName(hostname);
      LOG.debug("Deleting pod: {}.{}", namespace, podName);
      api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
    } catch (JsonSyntaxException e) {
      handleJsonSyntaxException(e);
    } catch (ApiException e) {
      throw convertApiException(e);
    }
  }

  /**
   * See <a href="https://github.com/kubernetes-client/java/issues/86">kubernetes-client/java#86</a>
   */
  private void handleJsonSyntaxException(JsonSyntaxException e) throws JsonSyntaxException {
    if (!(e.getCause() instanceof IllegalStateException)) {
      throw e;
    }
    final IllegalStateException ise = (IllegalStateException) e.getCause();
    if (
      ise.getMessage() != null && ise.getMessage().contains("BEGIN_OBJECT")
        && ise.getMessage().contains("Expected")
    ) {
      LOG.info("Operation probably succeeded but parse of result failed, "
        + "see https://github.com/kubernetes-client/java/issues/86");
    } else {
      throw e;
    }
  }

  private static IOException convertApiException(final ApiException e) {
    return new IOException(
      "response body: " + e.getResponseBody() + ", response code: " + e.getCode(), e);
  }

  private static RuntimeException unsupportedActionType(final String action) {
    return new RuntimeException("Unable to service request for action=" + action);
  }

  private static void assertSupportedServiceType(final ServiceType serviceType) {
    if (!SUPPORTED_SERVICE_TYPES.contains(serviceType)) {
      throw new RuntimeException("Unsupported ServiceType " + serviceType);
    }
  }
}
