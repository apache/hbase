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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/**
 * Tests for global coprocessor loading configuration
 */
@Category({CoprocessorTests.class, SmallTests.class})
public class TestCoprocessorConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorConfiguration.class);

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Configuration CONF = HBaseConfiguration.create();
  static {
    CONF.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      SystemCoprocessor.class.getName());
    CONF.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      SystemCoprocessor.class.getName());
    CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      SystemCoprocessor.class.getName());
  }
  private static final TableName TABLENAME = TableName.valueOf("TestCoprocessorConfiguration");
  private static final HRegionInfo REGIONINFO = new HRegionInfo(TABLENAME);
  private static final HTableDescriptor TABLEDESC = new HTableDescriptor(TABLENAME);
  static {
    try {
      TABLEDESC.addCoprocessor(TableCoprocessor.class.getName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // use atomic types in case coprocessor loading is ever multithreaded, also
  // so we can mutate them even though they are declared final here
  private static final AtomicBoolean systemCoprocessorLoaded = new AtomicBoolean();
  private static final AtomicBoolean tableCoprocessorLoaded = new AtomicBoolean();

  public static class SystemCoprocessor implements MasterCoprocessor, RegionCoprocessor,
      RegionServerCoprocessor {
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      systemCoprocessorLoaded.set(true);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException { }
  }

  public static class TableCoprocessor implements RegionCoprocessor {
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      tableCoprocessorLoaded.set(true);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException { }
  }

  @Test
  public void testRegionCoprocessorHostDefaults() throws Exception {
    Configuration conf = new Configuration(CONF);
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(REGIONINFO);
    when(region.getTableDescriptor()).thenReturn(TABLEDESC);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    systemCoprocessorLoaded.set(false);
    tableCoprocessorLoaded.set(false);
    new RegionCoprocessorHost(region, rsServices, conf);
    assertEquals("System coprocessors loading default was not honored",
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED, systemCoprocessorLoaded.get());
    assertEquals("Table coprocessors loading default was not honored",
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED &&
        CoprocessorHost.DEFAULT_USER_COPROCESSORS_ENABLED, tableCoprocessorLoaded.get());
  }

  @Test
  public void testRegionServerCoprocessorHostDefaults() throws Exception {
    Configuration conf = new Configuration(CONF);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    systemCoprocessorLoaded.set(false);
    new RegionServerCoprocessorHost(rsServices, conf);
    assertEquals("System coprocessors loading default was not honored",
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED, systemCoprocessorLoaded.get());
  }

  @Test
  public void testMasterCoprocessorHostDefaults() throws Exception {
    Configuration conf = new Configuration(CONF);
    MasterServices masterServices = mock(MasterServices.class);
    systemCoprocessorLoaded.set(false);
    new MasterCoprocessorHost(masterServices, conf);
    assertEquals("System coprocessors loading default was not honored",
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED, systemCoprocessorLoaded.get());
  }

  @Test
  public void testRegionCoprocessorHostAllDisabled() throws Exception {
    Configuration conf = new Configuration(CONF);
    conf.setBoolean(CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY, false);
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(REGIONINFO);
    when(region.getTableDescriptor()).thenReturn(TABLEDESC);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    systemCoprocessorLoaded.set(false);
    tableCoprocessorLoaded.set(false);
    new RegionCoprocessorHost(region, rsServices, conf);
    assertFalse("System coprocessors should not have been loaded",
      systemCoprocessorLoaded.get());
    assertFalse("Table coprocessors should not have been loaded",
      tableCoprocessorLoaded.get());
  }

  @Test
  public void testRegionCoprocessorHostTableLoadingDisabled() throws Exception {
    Configuration conf = new Configuration(CONF);
    conf.setBoolean(CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY, true); // if defaults change
    conf.setBoolean(CoprocessorHost.USER_COPROCESSORS_ENABLED_CONF_KEY, false);
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(REGIONINFO);
    when(region.getTableDescriptor()).thenReturn(TABLEDESC);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    systemCoprocessorLoaded.set(false);
    tableCoprocessorLoaded.set(false);
    new RegionCoprocessorHost(region, rsServices, conf);
    assertTrue("System coprocessors should have been loaded",
      systemCoprocessorLoaded.get());
    assertFalse("Table coprocessors should not have been loaded",
      tableCoprocessorLoaded.get());
  }

  /**
   * Rough test that Coprocessor Environment is Read-Only.
   * Just check a random CP and see that it returns a read-only config.
   */
  @Test
  public void testReadOnlyConfiguration() throws Exception {
    Configuration conf = new Configuration(CONF);
    HRegion region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(REGIONINFO);
    when(region.getTableDescriptor()).thenReturn(TABLEDESC);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    RegionCoprocessorHost rcp = new RegionCoprocessorHost(region, rsServices, conf);
    boolean found = false;
    for (String cpStr: rcp.getCoprocessors()) {
      CoprocessorEnvironment cpenv = rcp.findCoprocessorEnvironment(cpStr);
      if (cpenv != null) {
        found = true;
      }
      Configuration c = cpenv.getConfiguration();
      thrown.expect(UnsupportedOperationException.class);
      c.set("one.two.three", "four.five.six");
    }
    assertTrue("Should be at least one CP found", found);
  }
}
