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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestStoreFileTrackingUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileTrackingUtils.class);

  private Configuration conf;
  private boolean isFeatureEnabled;

  @Before
  public void setup() throws IOException {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testIsStoreFileTrackingPersistEnabled() {
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, true);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, PersistedStoreEngine.class.getName());
    isFeatureEnabled = StoreFileTrackingUtils.isStoreFileTrackingPersistEnabled(conf);
    assertTrue(isFeatureEnabled);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsStorefileTrackingPersistDisabledWithStoreEngineSet() {
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, false);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, PersistedStoreEngine.class.getName());
    isFeatureEnabled = StoreFileTrackingUtils.isStoreFileTrackingPersistEnabled(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testisStoreFileTrackingPersistEnabledWithMismatchedStoreEngine() {
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, true);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    isFeatureEnabled = StoreFileTrackingUtils.isStoreFileTrackingPersistEnabled(conf);
  }

  @Test
  public void testIsStorefileTrackingPersistDisabled() {
    conf.setBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED, false);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    isFeatureEnabled = StoreFileTrackingUtils.isStoreFileTrackingPersistEnabled(conf);
    assertFalse(isFeatureEnabled);
  }

  @Test
  public void testGetFamilyFromKey() {
    String separator = "-";
    String rowkey1 = "region-cf-table";
    String rowkey2 = "region-new-cf-table";
    assertEquals("cf",
      StoreFileTrackingUtils.getFamilyFromKey(rowkey1, "table", "region", separator));
    assertEquals("new-cf",
      StoreFileTrackingUtils.getFamilyFromKey(rowkey2, "table", "region", separator));
  }

}
