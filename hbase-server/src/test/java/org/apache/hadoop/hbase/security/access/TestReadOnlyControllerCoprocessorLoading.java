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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestReadOnlyControllerCoprocessorLoading {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerCoprocessorLoading.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReadOnlyController.class);
  private HBaseTestingUtil TEST_UTIL;

  Configuration conf;
  TableName tableName = TableName.valueOf("test_table");
  HMaster master;
  HRegionServer regionServer;
  HRegion region;

  private final boolean initialReadOnlyMode;

  public TestReadOnlyControllerCoprocessorLoading(boolean initialReadOnlyMode) {
    this.initialReadOnlyMode = initialReadOnlyMode;
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    if (TEST_UTIL.getMiniHBaseCluster() != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void setupMiniCluster(boolean isReadOnlyEnabled) throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, isReadOnlyEnabled);
    TEST_UTIL.startMiniCluster(1);

    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
  }

  private void createTable() throws Exception {
    // create a table to get at a region
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    TEST_UTIL.getAdmin().createTable(desc);

    List<HRegion> regions = regionServer.getRegions(tableName);
    assertFalse(regions.isEmpty());
    region = regions.get(0);
  }

  private void setReadOnlyMode(boolean isReadOnlyEnabled) {
    // Set the read-only enabled config dynamically after cluster startup
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, isReadOnlyEnabled);
    master.getConfigurationManager().notifyAllObservers(conf);
    regionServer.getConfigurationManager().notifyAllObservers(conf);
  }

  private void verifyMasterReadOnlyControllerLoading(boolean isReadOnlyEnabled) throws Exception {
    MasterCoprocessorHost masterCPHost = master.getMasterCoprocessorHost();
    if (isReadOnlyEnabled) {
      assertNotNull(
        MasterReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.",
        masterCPHost.findCoprocessor(MasterReadOnlyController.class.getName()));
    } else {
      assertNull(
        MasterReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false.",
        masterCPHost.findCoprocessor(MasterReadOnlyController.class.getName()));
    }
  }

  private void verifyRegionServerReadOnlyControllerLoading(boolean isReadOnlyEnabled)
    throws Exception {
    RegionServerCoprocessorHost rsCPHost = regionServer.getRegionServerCoprocessorHost();
    if (isReadOnlyEnabled) {
      assertNotNull(
        RegionServerReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.",
        rsCPHost.findCoprocessor(RegionServerReadOnlyController.class.getName()));
    } else {
      assertNull(
        RegionServerReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false.",
        rsCPHost.findCoprocessor(RegionServerReadOnlyController.class.getName()));
    }
  }

  private void verifyRegionReadOnlyControllerLoading(boolean isReadOnlyEnabled) throws Exception {
    RegionCoprocessorHost regionCPHost = region.getCoprocessorHost();

    if (isReadOnlyEnabled) {
      assertNotNull(
        RegionReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.",
        regionCPHost.findCoprocessor(RegionReadOnlyController.class.getName()));
      assertNotNull(
        EndpointReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.",
        regionCPHost.findCoprocessor(EndpointReadOnlyController.class.getName()));
      assertNotNull(
        BulkLoadReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.",
        regionCPHost.findCoprocessor(BulkLoadReadOnlyController.class.getName()));
    } else {
      assertNull(
        RegionReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false",
        regionCPHost.findCoprocessor(RegionReadOnlyController.class.getName()));
      assertNull(
        EndpointReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false",
        regionCPHost.findCoprocessor(EndpointReadOnlyController.class.getName()));
      assertNull(
        BulkLoadReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false",
        regionCPHost.findCoprocessor(BulkLoadReadOnlyController.class.getName()));
    }
  }

  private void verifyReadOnlyState(boolean isReadOnlyEnabled) throws Exception {
    verifyMasterReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionServerReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionReadOnlyControllerLoading(isReadOnlyEnabled);
  }

  @Test
  public void testReadOnlyControllerStartupBehavior() throws Exception {
    setupMiniCluster(initialReadOnlyMode);
    // Table creation is needed to get a region and verify region coprocessor loading hence we can't
    // test region coprocessor loading at startup.
    // This will get covered in the dynamic loading test where we will also verify that the
    // coprocessors are loaded at after table creation dynamically.
    verifyMasterReadOnlyControllerLoading(initialReadOnlyMode);
    verifyRegionServerReadOnlyControllerLoading(initialReadOnlyMode);
  }

  @Test
  public void testReadOnlyControllerLoadedWhenEnabledDynamically() throws Exception {
    setupMiniCluster(initialReadOnlyMode);
    if (!initialReadOnlyMode) {
      createTable();
    }
    boolean isReadOnlyEnabled = true;
    setReadOnlyMode(isReadOnlyEnabled);
    verifyMasterReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionServerReadOnlyControllerLoading(isReadOnlyEnabled);
    if (!initialReadOnlyMode) {
      verifyRegionReadOnlyControllerLoading(isReadOnlyEnabled);
    }
  }

  @Test
  public void testReadOnlyControllerUnloadedWhenDisabledDynamically() throws Exception {
    setupMiniCluster(initialReadOnlyMode);
    boolean isReadOnlyEnabled = false;
    setReadOnlyMode(isReadOnlyEnabled);
    createTable();
    verifyMasterReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionServerReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionReadOnlyControllerLoading(isReadOnlyEnabled);
  }

  @Test
  public void testReadOnlyControllerLoadUnloadedWhenMultipleReadOnlyToggle() throws Exception {
    setupMiniCluster(initialReadOnlyMode);

    // Ensure region exists before validation
    setReadOnlyMode(false);
    createTable();
    verifyReadOnlyState(false);

    // Define toggle sequence
    boolean[] toggleSequence = new boolean[] { true, false, // basic toggle
      true, true, // idempotent enable
      false, false // idempotent disable
    };

    for (int i = 0; i < toggleSequence.length; i++) {
      boolean state = toggleSequence[i];
      LOG.info("Toggling read-only mode to {} (step {})", state, i);

      setReadOnlyMode(state);
      verifyReadOnlyState(state);
    }
  }

  @Parameters(name = "initialReadOnlyMode={0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } });
  }
}
