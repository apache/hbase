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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.HbckChore;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.http.gson.GsonFactory;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.master.janitor.Report;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.reflect.TypeToken;

/**
 * Tests for the master hbck servlet.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestMasterHbckServlet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterHbckServlet.class);

  static final ServerName FAKE_HOST = ServerName.valueOf("fakehost", 12345, 1234567890);
  static final ServerName FAKE_HOST_2 = ServerName.valueOf("fakehost2", 12345, 1234567890);
  static final TableDescriptor FAKE_TABLE =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("mytable")).build();
  static final RegionInfo FAKE_HRI = RegionInfoBuilder.newBuilder(FAKE_TABLE.getTableName())
    .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
  static final RegionInfo FAKE_HRI_2 = RegionInfoBuilder.newBuilder(FAKE_TABLE.getTableName())
    .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("c")).build();
  static final RegionInfo FAKE_HRI_3 = RegionInfoBuilder.newBuilder(FAKE_TABLE.getTableName())
    .setStartKey(Bytes.toBytes("d")).setEndKey(Bytes.toBytes("e")).build();
  static final Path FAKE_PATH = new Path(
    "/hbase/data/default/" + FAKE_TABLE.getTableName() + "/" + FAKE_HRI_3.getEncodedName());
  static final long FAKE_START_TIMESTAMP = System.currentTimeMillis();
  static final long FAKE_END_TIMESTAMP = System.currentTimeMillis() + 1000;
  static final Gson GSON = GsonFactory.buildGson();

  private HMaster master;

  @Before
  public void setupMocks() {
    // Fake inconsistentRegions
    Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions = new HashMap<>();
    inconsistentRegions.put(FAKE_HRI.getEncodedName(),
      new Pair<>(FAKE_HOST, Arrays.asList(FAKE_HOST_2)));

    // Fake orphanRegionsOnRS
    Map<String, ServerName> orphanRegionsOnRS = new HashMap<>();

    // Fake orphanRegionsOnFS
    Map<String, Path> orphanRegionsOnFS = new HashMap<>();
    orphanRegionsOnFS.put(FAKE_HRI_3.getEncodedName(), FAKE_PATH);

    // Fake HbckChore
    HbckChore hbckChore = mock(HbckChore.class);
    doReturn(FAKE_START_TIMESTAMP).when(hbckChore).getCheckingStartTimestamp();
    doReturn(FAKE_END_TIMESTAMP).when(hbckChore).getCheckingEndTimestamp();
    doReturn(inconsistentRegions).when(hbckChore).getInconsistentRegions();
    doReturn(orphanRegionsOnRS).when(hbckChore).getOrphanRegionsOnRS();
    doReturn(orphanRegionsOnFS).when(hbckChore).getOrphanRegionsOnFS();

    // Fake region holes
    List<Pair<RegionInfo, RegionInfo>> holes = new ArrayList<>();

    // Fake region overlaps
    List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();
    overlaps.add(new Pair<>(FAKE_HRI, FAKE_HRI_2));

    // Fake unknown servers
    List<Pair<RegionInfo, ServerName>> unknownServers = new ArrayList<>();
    unknownServers.add(new Pair<>(FAKE_HRI, FAKE_HOST_2));

    // Fake empty region info
    List<String> emptyRegionInfo = new ArrayList<>();
    emptyRegionInfo.add(FAKE_HRI_3.getEncodedName());

    // Fake catalog janitor report
    Report report = mock(Report.class);
    doReturn(FAKE_START_TIMESTAMP).when(report).getCreateTime();
    doReturn(holes).when(report).getHoles();
    doReturn(overlaps).when(report).getOverlaps();
    doReturn(unknownServers).when(report).getUnknownServers();
    doReturn(emptyRegionInfo).when(report).getEmptyRegionInfo();

    // Fake CatalogJanitor
    CatalogJanitor janitor = mock(CatalogJanitor.class);
    doReturn(report).when(janitor).getLastReport();

    // Fake master

    master = mock(HMaster.class);
    doReturn(HBaseConfiguration.create()).when(master).getConfiguration();
    doReturn(true).when(master).isInitialized();
    doReturn(Optional.of(FAKE_HOST)).when(master).getActiveMaster();
    doReturn(janitor).when(master).getCatalogJanitor();
    doReturn(hbckChore).when(master).getHbckChore();
    MasterRpcServices services = mock(MasterRpcServices.class);
    doReturn(services).when(master).getRpcServices();
    doReturn(services).when(master).getMasterRpcServices();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testHbckServletWithMocks() throws Exception {
    // Set up request and response mocks
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(out);
    doReturn(writer).when(response).getWriter();

    // Set up servlet config
    ServletContext context = mock(ServletContext.class);
    Map<String, Object> configMap = new HashMap<>();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return configMap.get(invocation.getArgument(0));
      }
    }).when(context).getAttribute(anyString());
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        configMap.put(invocation.getArgument(0), invocation.getArgument(1));
        return null;
      }
    }).when(context).setAttribute(anyString(), any());
    ServletConfig config = mock(ServletConfig.class);
    doReturn(context).when(config).getServletContext();

    // Instantiate the servlet and call doGet
    MasterHbckServlet servlet = new MasterHbckServlet();
    servlet.init(config);
    servlet.getServletContext().setAttribute(HMaster.MASTER, master);
    servlet.doGet(request, response);
    // Flush the writer
    writer.flush();

    // Response should be now be in 'out'
    String responseString = new String(out.toByteArray(), StandardCharsets.UTF_8);
    Map<String, Object> result =
      GSON.fromJson(responseString, new TypeToken<Map<String, Object>>() {
      }.getType());

    // Check that the result is as expected
    long startTimestamp = ((Double) result.get(MasterHbckServlet.START_TIMESTAMP)).longValue();
    assertEquals(FAKE_START_TIMESTAMP, startTimestamp);
    long endTimestamp = ((Double) result.get(MasterHbckServlet.END_TIMESTAMP)).longValue();
    assertEquals(FAKE_END_TIMESTAMP, endTimestamp);
    Map<String, Object> inconsistentRegions =
      (Map<String, Object>) result.get(MasterHbckServlet.INCONSISTENT_REGIONS);
    assertNotNull(inconsistentRegions);
    assertEquals(1, inconsistentRegions.size());
    assertNotNull(inconsistentRegions.get(FAKE_HRI.getEncodedName()));
    assertNull(inconsistentRegions.get(FAKE_HRI_3.getEncodedName()));
    Map<String, Object> orphanRegionsOnRS =
      (Map<String, Object>) result.get(MasterHbckServlet.ORPHAN_REGIONS_ON_RS);
    assertNull(orphanRegionsOnRS);
    Map<String, String> orphanRegionsOnFS =
      (Map<String, String>) result.get(MasterHbckServlet.ORPHAN_REGIONS_ON_FS);
    assertNotNull(orphanRegionsOnFS);
    assertEquals(1, orphanRegionsOnFS.size());
    assertNull(orphanRegionsOnFS.get(FAKE_HRI.getEncodedName()));
    assertNotNull(orphanRegionsOnFS.get(FAKE_HRI_3.getEncodedName()));
    List holes = (List) result.get(MasterHbckServlet.HOLES);
    assertNull(holes);
    List overlaps = (List) result.get(MasterHbckServlet.OVERLAPS);
    assertNotNull(overlaps);
    assertEquals(1, overlaps.size());
    List unknownServers = (List) result.get(MasterHbckServlet.UNKNOWN_SERVERS);
    assertNotNull(unknownServers);
    assertEquals(1, unknownServers.size());
    List emptyRegionInfo = (List) result.get(MasterHbckServlet.EMPTY_REGIONINFO);
    assertNotNull(emptyRegionInfo);
    assertEquals(1, emptyRegionInfo.size());
  }

}
