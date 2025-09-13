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
package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.junit.After;
import org.junit.Before;

public class ManagedKeyTestBase {
  protected HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
      getKeyProviderClass().getName());
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.master.classes",
      KeymetaServiceEndpoint.class.getName());

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    TEST_UTIL.waitUntilAllRegionsAssigned(KeymetaTableAccessor.KEY_META_TABLE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected Class<? extends ManagedKeyProvider> getKeyProviderClass() {
    return MockManagedKeyProvider.class;
  }
}
