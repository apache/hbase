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

import static org.apache.hadoop.hbase.client.RegionInfoBuilder.FIRST_META_REGIONINFO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.hbck.HbckChore;
import org.apache.hadoop.hbase.master.hbck.HbckReport;
import org.apache.hadoop.hbase.master.http.hbck.resource.HbckMetricsResource;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitorReport;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.NotAcceptableException;
import org.apache.hbase.thirdparty.javax.ws.rs.client.Client;
import org.apache.hbase.thirdparty.javax.ws.rs.client.ClientBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.client.WebTarget;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;

/**
 * Tests for the {@link HbckMetricsResource}.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestHbckMetricsResource {

  private static final Logger LOG = LoggerFactory.getLogger(TestHbckMetricsResource.class);

  // Test data for Mock HBCK Report
  private static final long reportStartTime = 123456789000L;
  private static final long reportEndTime = 234567890000L;
  private static final String regionId1 = "regionId1";
  private static final String regionId2 = "regionId2";
  private static final String localhost1 = "localhost1";
  private static final String localhost2 = "localhost2";
  private static final String port = "16010";
  private static final String hostStartCode = "123456789";
  private static final String path1 = "hdfs://path1";
  private static final String path2 = "hdfs://path2";
  private static final String metaRegionID = FIRST_META_REGIONINFO.getEncodedName();
  private static final String metaTableName = FIRST_META_REGIONINFO.getTable().getNameAsString();

  // Various Keys in HBCK JSON Response.
  private static final String quoteColon = "\":";
  private static final String quote = "\"";
  private static final String regionId = quote + "region_id" + quoteColon;
  private static final String regionHdfsPath = quote + "region_hdfs_path" + quoteColon;
  private static final String rsName = quote + "rs_name" + quoteColon;
  private static final String hostName = quote + "host_name" + quoteColon;
  private static final String hostPort = quote + "host_port" + quoteColon;
  private static final String startCode = quote + "start_code" + quoteColon;
  private static final String serverNameInMeta = quote + "server_name_in_meta" + quoteColon;
  private static final String listOfServers = quote + "list_of_servers" + quoteColon;
  private static final String region1Info = quote + "region1_info" + quoteColon;
  private static final String region2Info = quote + "region2_info" + quoteColon;
  private static final String regionInfo = quote + "region_info" + quoteColon;
  private static final String serverName = quote + "server_name" + quoteColon;
  private static final String tableName = quote + "table_name" + quoteColon;

  private static final String dataStartsWith = "{\"data\":[";
  private static final String dataEndsWith = "]}";
  private static final String hbckReportStartTime = quote + "hbck_report_start_time" + quoteColon;
  private static final String hbckReportEndTime = quote + "hbck_report_end_time" + quoteColon;
  private static final String hbckOrphanRegionOnFS =
    quote + "hbck_orphan_regions_on_fs" + quoteColon;
  private static final String hbckOrphanRegionOnRS =
    quote + "hbck_orphan_regions_on_rs" + quoteColon;
  private static final String hbckInconsistentRegion =
    quote + "hbck_inconsistent_regions" + quoteColon;
  private static final String hbckHoles = quote + "hbck_holes" + quoteColon;
  private static final String hbckOverlaps = quote + "hbck_overlaps" + quoteColon;
  private static final String hbckUnknownServers = quote + "hbck_unknown_servers" + quoteColon;
  private static final String hbckEmptyRegionInfo = quote + "hbck_empty_region_info" + quoteColon;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHbckMetricsResource.class);

  private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(
      StartMiniClusterOption.builder().numZkServers(3).numMasters(3).numDataNodes(3).build())
    .setConfiguration(() -> {
      // enable Master InfoServer and random port selection
      final Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.MASTER_INFO_PORT, 0);
      conf.set("hbase.http.jersey.tracing.type", "ON_DEMAND");
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

      HMaster master = miniClusterRule.getTestingUtility().getMiniHBaseCluster().getMaster();

      HbckChore hbckChore = mock(HbckChore.class);
      HbckReport hbckReport = mock(HbckReport.class);
      CatalogJanitor catalogJanitorChore = mock(CatalogJanitor.class);
      CatalogJanitorReport catalogJanitorReport = mock(CatalogJanitorReport.class);
      master.setHbckChoreForTesting(hbckChore);
      master.setCatalogJanitorChoreForTesting(catalogJanitorChore);

      // Test data for Mock HBCK Report
      ServerName server1 =
        ServerName.valueOf(localhost1, Integer.parseInt(port), Integer.parseInt(hostStartCode));
      ServerName server2 =
        ServerName.valueOf(localhost2, Integer.parseInt(port), Integer.parseInt(hostStartCode));
      Path hdfsPath1 = new Path(path1);
      Path hdfsPath2 = new Path(path2);

      // Orphan on RS Test data
      Map<String, ServerName> mapOfOrphanRegionsOnRS = new HashMap<>();
      mapOfOrphanRegionsOnRS.put(regionId1, server1);
      mapOfOrphanRegionsOnRS.put(regionId2, server2);

      // Orphan Region on FS Test Data
      Map<String, Path> mapOfOrphanRegionOnFS = new HashMap<>();
      mapOfOrphanRegionOnFS.put(regionId1, hdfsPath1);
      mapOfOrphanRegionOnFS.put(regionId2, hdfsPath2);

      // Inconsistent Regions Test Data
      Map<String, Pair<ServerName, List<ServerName>>> mapOfInconsistentRegions = new HashMap<>();
      mapOfInconsistentRegions.put(regionId1, new Pair<>(server1, Arrays.asList(server1, server2)));
      mapOfInconsistentRegions.put(regionId2, new Pair<>(server2, Arrays.asList(server1, server2)));

      // Region Overlap and Region Holes Test Data
      List<Pair<RegionInfo, RegionInfo>> listOfRegion = new ArrayList<>();
      listOfRegion.add(new Pair<>(FIRST_META_REGIONINFO, FIRST_META_REGIONINFO));
      listOfRegion.add(new Pair<>(FIRST_META_REGIONINFO, FIRST_META_REGIONINFO));

      // Unknown RegionServer Test Data
      List<Pair<RegionInfo, ServerName>> listOfUnknownServers = new ArrayList<>();
      listOfUnknownServers.add(new Pair<>(FIRST_META_REGIONINFO, server1));
      listOfUnknownServers.add(new Pair<>(FIRST_META_REGIONINFO, server2));

      // Empty Region Info Test Data
      List<byte[]> listOfEmptyRegionInfo = new ArrayList<>();
      listOfEmptyRegionInfo.add(regionId1.getBytes());
      listOfEmptyRegionInfo.add(regionId2.getBytes());

      // Mock HBCK Report and CatalogJanitor Report
      when(hbckReport.getCheckingStartTimestamp())
        .thenReturn(Instant.ofEpochMilli(reportStartTime));
      when(hbckReport.getCheckingEndTimestamp()).thenReturn(Instant.ofEpochSecond(reportEndTime));
      when(hbckReport.getOrphanRegionsOnFS()).thenReturn(mapOfOrphanRegionOnFS);
      when(hbckReport.getOrphanRegionsOnRS()).thenReturn(mapOfOrphanRegionsOnRS);
      when(hbckReport.getInconsistentRegions()).thenReturn(mapOfInconsistentRegions);
      when(catalogJanitorReport.getHoles()).thenReturn(listOfRegion);
      when(catalogJanitorReport.getOverlaps()).thenReturn(listOfRegion);
      when(catalogJanitorReport.getUnknownServers()).thenReturn(listOfUnknownServers);
      when(catalogJanitorReport.getEmptyRegionInfo()).thenReturn(listOfEmptyRegionInfo);

      Mockito.doReturn(hbckReport).when(hbckChore).getLastReport();
      Mockito.doReturn(catalogJanitorReport).when(catalogJanitorChore).getLastReport();

      final String baseUrl =
        admin.getMaster().thenApply(ServerName::getHostname).thenCombine(admin.getMasterInfoPort(),
          (hostName, infoPort) -> "http://" + hostName + ":" + infoPort).get();
      final Client client = ClientBuilder.newClient();
      target = client.target(baseUrl).path("hbck/hbck-metrics");
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
    LOG.info("HBCK JSON Response : " + response);
    assertThat(response,
      allOf(containsString(hbckReportStartTime), containsString(hbckReportEndTime),
        containsString(hbckOrphanRegionOnFS), containsString(hbckOrphanRegionOnRS),
        containsString(hbckInconsistentRegion), containsString(hbckHoles),
        containsString(hbckOverlaps), containsString(hbckUnknownServers),
        containsString(hbckEmptyRegionInfo), containsString(Objects.toString(reportStartTime)),
        containsString(Objects.toString(reportEndTime))));
  }

  @Test
  public void testGetRootHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget()
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetOrphanRegionOnFS() {
    final String response =
      classRule.getTarget().path("orphan-regions-on-fs").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource orphan-regions-on-fs : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(regionId),
        containsString(regionHdfsPath), containsString(regionId1), containsString(regionId2),
        containsString(path1), containsString(path2)));
  }

  @Test
  public void testGetOrphanRegionOnFSHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("orphan-regions-on-fs").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetOrphanRegionOnRS() {
    final String response =
      classRule.getTarget().path("orphan-regions-on-rs").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource orphan-regions-on-rs : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(regionId),
        containsString(rsName), containsString(hostName), containsString(hostPort),
        containsString(startCode), containsString(regionId1), containsString(regionId2),
        containsString(localhost1), containsString(localhost2), containsString(port),
        containsString(hostStartCode)));
  }

  @Test
  public void testGetOrphanRegionOnRSHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("orphan-regions-on-rs").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetInconsistentRegions() {
    final String response =
      classRule.getTarget().path("inconsistent-regions").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource inconsistent-regions : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(hostName),
        containsString(hostPort), containsString(startCode), containsString(listOfServers),
        containsString(regionId1), containsString(regionId2), containsString(regionId),
        containsString(serverNameInMeta), containsString(localhost1), containsString(localhost2),
        containsString(port), containsString(hostStartCode)));
  }

  @Test
  public void testGetInconsistentRegionsHtml() {
    assertThrows(NotAcceptableException.class,
      () -> classRule.getTarget().path("inconsistent-regions").request(MediaType.TEXT_HTML_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetRegionHoles() {
    final String response =
      classRule.getTarget().path("region-holes").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource region-holes : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(region1Info),
        containsString(region2Info), containsString(regionId), containsString(tableName),
        containsString(metaRegionID), containsString(metaTableName)));
  }

  @Test
  public void testGetRegionHolesHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("region-holes")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetRegionOverlaps() {
    final String response =
      classRule.getTarget().path("region-overlaps").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource region-overlaps : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(regionId),
        containsString(tableName), containsString(region2Info), containsString(region2Info),
        containsString(metaRegionID), containsString(metaTableName)));
  }

  @Test
  public void testGetRegionOverlapsHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("region-overlaps")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetUnkownServers() {
    final String response =
      classRule.getTarget().path("unknown-servers").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource unknown-servers : " + response);
    assertThat(response,
      allOf(startsWith(dataStartsWith), endsWith(dataEndsWith), containsString(regionInfo),
        containsString(regionId), containsString(tableName), containsString(serverName),
        containsString(serverName), containsString(port), containsString(startCode),
        containsString(metaRegionID), containsString(metaTableName), containsString(localhost1),
        containsString(localhost2), containsString(port), containsString(startCode)));
  }

  @Test
  public void testGetUnkownServersHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("unknown-servers")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }

  @Test
  public void testGetEmptyRegionInfo() {
    final String response =
      classRule.getTarget().path("empty-regioninfo").request(MediaType.APPLICATION_JSON_TYPE)
        .header("X-Jersey-Tracing-Accept", true).get(String.class);
    LOG.info("HBCK Response for resource empty-regioninfo : " + response);
    assertThat(response, allOf(startsWith(dataStartsWith), endsWith(dataEndsWith),
      containsString(regionInfo), containsString(regionId1), containsString(regionId2)));
  }

  @Test
  public void testGetEmptyRegionInfoHtml() {
    assertThrows(NotAcceptableException.class, () -> classRule.getTarget().path("empty-regioninfo")
      .request(MediaType.TEXT_HTML_TYPE).header("X-Jersey-Tracing-Accept", true).get(String.class));
  }
}
