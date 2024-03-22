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
import org.apache.hadoop.hbase.master.http.hbck.resource.HbckMetricsResource;
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
 * Tests for the master api_v1 {@link HbckMetricsResource}.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestHbckMetricsResource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHbckMetricsResource.class);

  private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(
      StartTestingClusterOption.builder().numZkServers(3).numMasters(3).numDataNodes(3).build())
    .setConfiguration(() -> {
      // enable Master InfoServer and random port selection
      final Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.MASTER_INFO_PORT, 0);
      conf.set("hbase.http.jersey.tracing.type", "ON_DEMAND");
      // If we don't set this value to higher like 5sec, hbck chore won't get a chance to run and
      // the all tests in this class will fail.
      conf.setInt("hbase.master.hbck.chore.interval", 1000);
      return conf;
    }).build();

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
      tableName = TableName.valueOf(TestHbckMetricsResource.class.getSimpleName());
    }

    public WebTarget getTarget() {
      return target;
    }

    @Override
    protected void before() throws Throwable {
      final AsyncConnection conn = connectionSupplier.get();
      admin = conn.getAdmin();
      final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("c")).build())
        .setDurability(Durability.SKIP_WAL).build();
      admin.createTable(tableDescriptor).get();

      final String baseUrl =
        admin.getMaster().thenApply(ServerName::getHostname).thenCombine(admin.getMasterInfoPort(),
          (hostName, infoPort) -> "http://" + hostName + ":" + infoPort).get();
      final Client client = ClientBuilder.newClient();
      target = client.target(baseUrl).path("hbck/hbck_metrics");
    }

    @Override
    protected void after() {
      final TableName tableName = TableName.valueOf("test");
      try {
        admin.tableExists(tableName).thenCompose(val -> {
          if (val) {
            return admin.disableTable(tableName)
              .thenCompose(ignored -> admin.deleteTable(tableName));
          } else {
            return CompletableFuture.completedFuture(null);
          }
        }).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @ClassRule
  public static RuleChain ruleChain =
    RuleChain.outerRule(miniClusterRule).around(connectionRule).around(classRule);

  @Test
  public void testGetRoot() {
    final String response = classRule.getTarget().request(MediaType.APPLICATION_JSON_TYPE)
      .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(containsString("\"hbck_report_start_time\":"),
      containsString("\"hbck_report_end_time\":"), containsString("\"hbck_orphan_regions_on_fs\":"),
      containsString("\"hbck_orphan_regions_on_rs\":"),
      containsString("\"hbck_inconsistent_regions\":"), containsString("\"hbck_holes\":"),
      containsString("\"hbck_overlaps\":"), containsString("\"hbck_unknown_servers\":"),
      containsString("\"hbck_empty_region_info\":")));
  }

  @Test
  public void testGetRootHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget()
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetOrphanRegionOnFS() {
    final String response =
      classRule.getTarget().path("orphan_regions_on_fs").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetOrphanRegionOnFSHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("orphan_regions_on_fs").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetOrphanRegionOnRS() {
    final String response =
      classRule.getTarget().path("orphan_regions_on_rs").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetOrphanRegionOnRSHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("orphan_regions_on_rs").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetInconsistentRegions() {
    final String response =
      classRule.getTarget().path("inconsistent_regions").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetInconsistentRegionsHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("inconsistent_regions").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetRegionHoles() {
    final String response =
      classRule.getTarget().path("region_holes").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetRegionHolesHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("region_holes")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetRegionOverlaps() {
    final String response =
      classRule.getTarget().path("region_overlaps").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetRegionOverlapsHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("region_overlaps")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetUnkownServers() {
    final String response =
      classRule.getTarget().path("unknown_servers").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetUnkownServersHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("unknown_servers")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetEmptyRegionInfo() {
    final String response =
      classRule.getTarget().path("empty_regioninfo").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    assertThat(response, allOf(startsWith("{\"data\":["), endsWith("]}")));
  }

  @Test
  public void testGetEmptyRegionInfoHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("empty_regioninfo")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }
}
