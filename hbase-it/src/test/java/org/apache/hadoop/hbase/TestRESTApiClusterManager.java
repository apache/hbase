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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager.ServiceType;
import org.apache.hadoop.hbase.RESTApiClusterManager.Service;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(SmallTests.TAG)
public class TestRESTApiClusterManager {

  public static MockHttpApiRule mockHttpApi = new MockHttpApiRule();

  private static HBaseCommonTestingUtil testingUtility;
  private ClusterManager clusterManager;

  @BeforeAll
  public static void beforeClass() throws Exception {
    mockHttpApi.start();
    testingUtility = new HBaseCommonTestingUtil();
    configureClusterManager(testingUtility.getConfiguration());
  }

  @AfterAll
  public static void afterClass() throws Exception {
    mockHttpApi.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    mockHttpApi.clearRegistrations();
    final Configuration methodConf = new Configuration(testingUtility.getConfiguration());
    methodConf.set("hbase.it.clustermanager.restapi.clustername",
      testInfo.getTestMethod().get().getName());
    clusterManager = new RESTApiClusterManager();
    clusterManager.setConf(methodConf);
  }

  @Test
  public void isRunningPositive(TestInfo testInfo) throws IOException {
    final String clusterName = testInfo.getTestMethod().get().getName();
    final String hostName = "somehost";
    final String serviceName = "hbase";
    final String hostId = "some-id";
    registerServiceName(clusterName, Service.HBASE, serviceName);
    registerHost(hostName, hostId);
    final Map<String, String> hostProperties = new HashMap<>();
    hostProperties.put("roleState", "STARTED");
    hostProperties.put("healthSummary", "GOOD");
    registerHostProperties(clusterName, serviceName, hostId, ServiceType.HBASE_MASTER,
      hostProperties);
    assertTrue(clusterManager.isRunning(ServiceType.HBASE_MASTER, hostName, -1));
  }

  private static void configureClusterManager(final Configuration conf) {
    conf.set("hbase.it.clustermanager.restapi.hostname", mockHttpApi.getURI().toString());
  }

  private static void registerServiceName(final String clusterName, final Service service,
    final String serviceName) {
    final String target = String.format("^/api/v6/clusters/%s/services", clusterName);
    final String response = String
      .format("{ \"items\": [ { \"type\": \"%s\", \"name\": \"%s\" } ] }", service, serviceName);
    mockHttpApi.registerOk(target, response);
  }

  private static void registerHost(final String hostName, final String hostId) {
    final String target = "^/api/v6/hosts";
    final String response = String
      .format("{ \"items\": [ { \"hostname\": \"%s\", \"hostId\": \"%s\" } ] }", hostName, hostId);
    mockHttpApi.registerOk(target, response);
  }

  private static void registerHostProperties(final String clusterName, final String serviceName,
    final String hostId, final ServiceType serviceType, final Map<String, String> properties) {
    final String target =
      String.format("^/api/v6/clusters/%s/services/%s/roles", clusterName, serviceName);
    final StringBuilder builder =
      new StringBuilder().append("{ \"items\": [ ").append("{ \"hostRef\": { \"hostId\": \"")
        .append(hostId).append("\" }, \"type\": \"").append(serviceType).append("\"");
    properties
      .forEach((k, v) -> builder.append(", \"").append(k).append("\": \"").append(v).append("\""));
    builder.append(" } ] }");
    mockHttpApi.registerOk(target, builder.toString());
  }
}
