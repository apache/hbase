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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestReadOnlyControllerCoprocessorLoading {

  private static final Logger LOG = LoggerFactory.getLogger(TestReadOnlyController.class);
  private HBaseTestingUtil TEST_UTIL;

  Configuration conf;
  TableName tableName = TableName.valueOf("test_table");
  HMaster master;
  HRegionServer regionServer;
  HRegion region;

  private boolean initialReadOnlyMode;

  public TestReadOnlyControllerCoprocessorLoading() {
    this.initialReadOnlyMode = false;
  }

  @BeforeEach
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    if (TEST_UTIL.getMiniHBaseCluster() != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @AfterEach
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

  private Configuration setReadOnlyMode(boolean isReadOnlyEnabled) {
    // Create a new configuration to mimic client server behavior
    // otherwise the existing conf object is shared with the cluster
    // and can cause side effects on other tests if not reset properly.
    // This way we can ensure that only the coprocessor loading is tested
    // without impacting other tests.
    HBaseTestingUtil NEW_TEST_UTIL = new HBaseTestingUtil();
    Configuration newConf = NEW_TEST_UTIL.getConfiguration();
    // Set the read-only enabled config dynamically after cluster startup
    newConf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, isReadOnlyEnabled);
    master.getConfigurationManager().notifyAllObservers(newConf);
    regionServer.getConfigurationManager().notifyAllObservers(newConf);
    if (region != null) {
      region.getConfigurationManager().notifyAllObservers(newConf);
    }
    return newConf;
  }

  private void verifyMasterReadOnlyControllerLoading(boolean isReadOnlyEnabled) {
    MasterCoprocessorHost masterCPHost = master.getMasterCoprocessorHost();
    if (isReadOnlyEnabled) {
      assertNotNull(masterCPHost.findCoprocessor(MasterReadOnlyController.class.getName()),
        MasterReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.");
    } else {
      assertNull(masterCPHost.findCoprocessor(MasterReadOnlyController.class.getName()),
        MasterReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false.");
    }
  }

  private void verifyRegionServerReadOnlyControllerLoading(boolean isReadOnlyEnabled) {
    RegionServerCoprocessorHost rsCPHost = regionServer.getRegionServerCoprocessorHost();
    if (isReadOnlyEnabled) {
      assertNotNull(rsCPHost.findCoprocessor(RegionServerReadOnlyController.class.getName()),
        RegionServerReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.");
    } else {
      assertNull(rsCPHost.findCoprocessor(RegionServerReadOnlyController.class.getName()),
        RegionServerReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false.");
    }
  }

  private void verifyRegionReadOnlyControllerLoading(boolean isReadOnlyEnabled) {
    RegionCoprocessorHost regionCPHost = region.getCoprocessorHost();

    if (isReadOnlyEnabled) {
      assertNotNull(regionCPHost.findCoprocessor(RegionReadOnlyController.class.getName()),
        RegionReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.");
      assertNotNull(regionCPHost.findCoprocessor(EndpointReadOnlyController.class.getName()),
        EndpointReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.");
      assertNotNull(regionCPHost.findCoprocessor(BulkLoadReadOnlyController.class.getName()),
        BulkLoadReadOnlyController.class.getName()
          + " should be loaded at startup when readonly is true.");
    } else {
      assertNull(regionCPHost.findCoprocessor(RegionReadOnlyController.class.getName()),
        RegionReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false");
      assertNull(regionCPHost.findCoprocessor(EndpointReadOnlyController.class.getName()),
        EndpointReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false");
      assertNull(regionCPHost.findCoprocessor(BulkLoadReadOnlyController.class.getName()),
        BulkLoadReadOnlyController.class.getName()
          + " should not be loaded at startup when readonly support property is false");
    }
  }

  private void verifyReadOnlyState(boolean isReadOnlyEnabled) throws Exception {
    verifyMasterReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionServerReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionReadOnlyControllerLoading(isReadOnlyEnabled);
  }

  @ParameterizedTest(name = "initialReadOnlyMode={0}")
  @MethodSource("parameters")
  public void testReadOnlyControllerStartupBehavior(boolean initialReadOnlyMode) throws Exception {
    this.initialReadOnlyMode = initialReadOnlyMode;
    setupMiniCluster(initialReadOnlyMode);
    // Table creation is needed to get a region and verify region coprocessor loading hence we can't
    // test region coprocessor loading at startup.
    // This will get covered in the dynamic loading test where we will also verify that the
    // coprocessors are loaded at after table creation dynamically.
    verifyMasterReadOnlyControllerLoading(initialReadOnlyMode);
    verifyRegionServerReadOnlyControllerLoading(initialReadOnlyMode);
  }

  @ParameterizedTest(name = "initialReadOnlyMode={0}")
  @MethodSource("parameters")
  public void testReadOnlyControllerLoadedWhenEnabledDynamically(boolean initialReadOnlyMode)
    throws Exception {
    this.initialReadOnlyMode = initialReadOnlyMode;
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

  @ParameterizedTest(name = "initialReadOnlyMode={0}")
  @MethodSource("parameters")
  public void testReadOnlyControllerUnloadedWhenDisabledDynamically(boolean initialReadOnlyMode)
    throws Exception {
    this.initialReadOnlyMode = initialReadOnlyMode;
    setupMiniCluster(initialReadOnlyMode);
    boolean isReadOnlyEnabled = false;
    Configuration newConf = setReadOnlyMode(isReadOnlyEnabled);
    createTable();
    // The newly created table's region has a stale conf that needs to be updated
    region.onConfigurationChange(newConf);
    verifyMasterReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionServerReadOnlyControllerLoading(isReadOnlyEnabled);
    verifyRegionReadOnlyControllerLoading(isReadOnlyEnabled);
  }

  @ParameterizedTest(name = "initialReadOnlyMode={0}")
  @MethodSource("parameters")
  public void testReadOnlyControllerLoadUnloadedWhenMultipleReadOnlyToggle(
    boolean initialReadOnlyMode) throws Exception {
    this.initialReadOnlyMode = initialReadOnlyMode;
    setupMiniCluster(initialReadOnlyMode);

    // Ensure region exists before validation
    Configuration newConf = setReadOnlyMode(false);
    createTable();
    // The newly created table's region has a stale conf that needs to be updated
    region.onConfigurationChange(newConf);
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

  static Stream<Boolean> parameters() {
    return Arrays.stream(new Boolean[] { Boolean.TRUE, Boolean.FALSE });
  }
}
