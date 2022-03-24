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
package org.apache.hadoop.hbase.master.http;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.http.api_v1.cluster_metrics.resource.ClusterMetricsResource;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.apache.hbase.thirdparty.javax.ws.rs.NotAcceptableException;
import org.apache.hbase.thirdparty.javax.ws.rs.client.Client;
import org.apache.hbase.thirdparty.javax.ws.rs.client.ClientBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.client.WebTarget;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

/**
 * Tests for the master api_v1 {@link ClusterMetricsResource}.
 */
@Category({ MasterTests.class, LargeTests.class})
public class TestApiV1ClusterMetricsResource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestApiV1ClusterMetricsResource.class);

  private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(StartTestingClusterOption.builder()
      .numZkServers(3)
      .numMasters(3)
      .numDataNodes(3)
      .build())
    .setConfiguration(() -> {
      // enable Master InfoServer and random port selection
      final Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.MASTER_INFO_PORT, 0);
      conf.set("hbase.http.jersey.tracing.type", "ON_DEMAND");
      return conf;
    })
    .build();
  private static final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);
  private static final ClassSetup classRule = new ClassSetup(connectionRule::getAsyncConnection);

  private static final class ClassSetup extends ExternalResource {

    private final Supplier<AsyncConnection> connectionSupplier;
    private final TableName tableName;
    private AsyncAdmin admin;
    private WebTarget target;

    public ClassSetup(final Supplier<AsyncConnection> connectionSupplier) {
      this.connectionSupplier = connectionSupplier;
      tableName = TableName.valueOf(TestApiV1ClusterMetricsResource.class.getSimpleName());
    }

    public WebTarget getTarget() {
      return target;
    }

    @Override
    protected void before() throws Throwable {
      final AsyncConnection conn = connectionSupplier.get();
      admin = conn.getAdmin();
      final TableDescriptor tableDescriptor = TableDescriptorBuilder
        .newBuilder(tableName)
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("c")).build())
        .setDurability(Durability.SKIP_WAL)
        .build();
      admin.createTable(tableDescriptor).get();

      final String baseUrl = admin.getMaster()
        .thenApply(ServerName::getHostname)
        .thenCombine(
          admin.getMasterInfoPort(),
          (hostName, infoPort) -> "http://" + hostName + ":" + infoPort)
        .get();
      final Client client = ClientBuilder.newClient();
      target = client.target(baseUrl).path("api/v1/admin/cluster_metrics");
    }

    @Override
    protected void after() {
      final TableName tableName = TableName.valueOf("test");
      try {
        admin.tableExists(tableName)
          .thenCompose(val -> {
            if (val) {
              return admin.disableTable(tableName)
                .thenCompose(ignored -> admin.deleteTable(tableName));
            } else {
              return CompletableFuture.completedFuture(null);
            }
          })
          .get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(miniClusterRule)
    .around(connectionRule)
    .around(classRule);

  @Test
  public void testGetRoot() {
    final String response = classRule.getTarget()
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class);
    assertThat(response, allOf(
      containsString("\"hbase_version\":"),
      containsString("\"cluster_id\":"),
      containsString("\"master_name\":"),
      containsString("\"backup_master_names\":")));
  }

  @Test
  public void testGetRootHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget()
      .request(MediaType.TEXT_HTML_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class));
  }

  @Test
  public void testGetLiveServers() {
    final String response = classRule.getTarget()
      .path("live_servers")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class);
    assertThat(response, allOf(
      startsWith("{\"data\":["),
      endsWith("]}")));
  }

  @Test
  public void testGetLiveServersHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget()
      .path("live_servers")
      .request(MediaType.TEXT_HTML_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class));
  }

  @Test
  public void testGetDeadServers() {
    final String response = classRule.getTarget()
      .path("dead_servers")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class);
    assertThat(response, allOf(
      startsWith("{\"data\":["),
      endsWith("]}")));
  }

  @Test
  public void testGetDeadServersHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget()
      .path("dead_servers")
      .request(MediaType.TEXT_HTML_TYPE)
      .header("X-Jersey-Tracing-Accept", true)
      .get(String.class));
  }
}
