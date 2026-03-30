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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.access.BulkLoadReadOnlyController;
import org.apache.hadoop.hbase.security.access.EndpointReadOnlyController;
import org.apache.hadoop.hbase.security.access.MasterReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionServerReadOnlyController;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, SmallTests.class })
public class TestCoprocessorConfigurationUtil {

  private Configuration conf;
  private String key;

  @Before
  public void setUp() {
    conf = new Configuration();
    key = "test.key";
  }

  @Test
  public void testAddCoprocessorsEmptyCPList() {
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1", "cp2"));
    assertArrayEquals(new String[] { "cp1", "cp2" }, conf.getStrings(key));
  }

  @Test
  public void testAddCoprocessorsNonEmptyCPList() {
    conf.setStrings(key, "cp1");
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1", "cp2"));
    assertArrayEquals(new String[] { "cp1", "cp2" }, conf.getStrings(key));
  }

  @Test
  public void testAddCoprocessorsNoChange() {
    conf.setStrings(key, "cp1");
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1"));
    assertArrayEquals(new String[] { "cp1" }, conf.getStrings(key));
  }

  @Test
  public void testAddCoprocessorsIdempotent() {
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1", "cp2"));
    // Call again
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1", "cp2"));
    assertArrayEquals(new String[] { "cp1", "cp2" }, conf.getStrings(key));
  }

  @Test
  public void testAddCoprocessorsIdempotentWithOverlap() {
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1"));
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp1", "cp2"));
    CoprocessorConfigurationUtil.addCoprocessors(conf, key, List.of("cp2"));
    assertArrayEquals(new String[] { "cp1", "cp2" }, conf.getStrings(key));
  }

  @Test
  public void testRemoveCoprocessorsEmptyCPList() {
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp1"));
    assertNull(conf.getStrings(key));
  }

  @Test
  public void testRemoveCoprocessorsNonEmptyCPList() {
    conf.setStrings(key, "cp1", "cp2", "cp3");
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    assertArrayEquals(new String[] { "cp1", "cp3" }, conf.getStrings(key));
  }

  @Test
  public void testRemoveCoprocessorsNoChange() {
    conf.setStrings(key, "cp1");
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    assertArrayEquals(new String[] { "cp1" }, conf.getStrings(key));
  }

  @Test
  public void testRemoveCoprocessorsIdempotent() {
    conf.setStrings(key, "cp1", "cp2");
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    // Call again
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    assertArrayEquals(new String[] { "cp1" }, conf.getStrings(key));
  }

  @Test
  public void testRemoveCoprocessorsIdempotentWhenNotPresent() {
    conf.setStrings(key, "cp1");
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    CoprocessorConfigurationUtil.removeCoprocessors(conf, key, List.of("cp2"));
    assertArrayEquals(new String[] { "cp1" }, conf.getStrings(key));
  }

  private void assertEnableReadOnlyMode(String key, List<String> expected) {
    conf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    String[] result = conf.getStrings(key);
    assertNotNull(result);
    assertEquals(expected.size(), result.length);
    assertTrue(Arrays.asList(result).containsAll(expected));
  }

  @Test
  public void testSyncReadOnlyConfigurationsReadOnlyEnableAllKeys() {
    assertEnableReadOnlyMode(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      List.of(MasterReadOnlyController.class.getName()));

    assertEnableReadOnlyMode(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      List.of(RegionServerReadOnlyController.class.getName()));
    assertEnableReadOnlyMode(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      List.of(RegionReadOnlyController.class.getName(), BulkLoadReadOnlyController.class.getName(),
        EndpointReadOnlyController.class.getName()));
  }

  private void assertDisableReadOnlyMode(String key, List<String> initialCoprocs) {
    conf.setStrings(key, initialCoprocs.toArray(new String[0]));
    conf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    String[] result = conf.getStrings(key);
    assertTrue(result == null || result.length == 0);
  }

  @Test
  public void testSyncReadOnlyConfigurationsReadOnlyDisableAllKeys() {
    assertDisableReadOnlyMode(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      List.of(MasterReadOnlyController.class.getName()));

    assertDisableReadOnlyMode(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      List.of(RegionServerReadOnlyController.class.getName()));

    assertDisableReadOnlyMode(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      List.of(RegionReadOnlyController.class.getName(), BulkLoadReadOnlyController.class.getName(),
        EndpointReadOnlyController.class.getName()));
  }

  @Test
  public void testSyncReadOnlyConfigurationsReadOnlyEnablePreservesExistingCoprocessors() {
    String key = CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
    conf.setStrings(key, "existingCp");
    conf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    List<String> result = Arrays.asList(conf.getStrings(key));
    assertTrue(result.contains("existingCp"));
    assertTrue(result.contains(MasterReadOnlyController.class.getName()));
  }

  @Test
  public void testSyncReadOnlyConfigurationsReadOnlyDisableRemovesOnlyReadOnlyCoprocessor() {
    Configuration conf = new Configuration(false);
    String key = CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
    String existingCp = "org.example.OtherCP";
    conf.setStrings(key, existingCp, MasterReadOnlyController.class.getName());
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    String[] cps = conf.getStrings(key);
    assertNotNull(cps);
    assertEquals(1, cps.length);
    assertEquals(existingCp, cps[0]);
  }

  @Test
  public void testSyncReadOnlyConfigurationsIsIdempotent() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    String key = CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    CoprocessorConfigurationUtil.syncReadOnlyConfigurations(conf, key);
    String[] cps = conf.getStrings(key);
    assertNotNull(cps);
    assertEquals(1, cps.length);
  }
}
