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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedKeyTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKeyTestBase.class);

  protected HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    if (isWithKeyManagement()) {
      TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
        getKeyProviderClass().getName());
      TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
      TEST_UTIL.getConfiguration().set("hbase.coprocessor.master.classes",
        KeymetaServiceEndpoint.class.getName());
    }

    // Start the minicluster if needed
    if (isWithMiniClusterStart()) {
      LOG.info("\n\nManagedKeyTestBase.setUp: Starting minicluster\n");
      startMiniCluster();
      LOG.info("\n\nManagedKeyTestBase.setUp: Minicluster successfully started\n");
    }
  }

  protected void startMiniCluster() throws Exception {
    startMiniCluster(getSystemTableNameToWaitFor());
  }

  protected void startMiniCluster(TableName tableNameToWaitFor) throws Exception {
    TEST_UTIL.startMiniCluster(1);
    waitForMasterInitialization(tableNameToWaitFor);
  }

  protected void restartMiniCluster() throws Exception {
    restartMiniCluster(getSystemTableNameToWaitFor());
  }

  protected void restartMiniCluster(TableName tableNameToWaitFor) throws Exception {
    LOG.info("\n\nManagedKeyTestBase.restartMiniCluster: Flushing caches\n");
    TEST_UTIL.flush();

    LOG.info("\n\nManagedKeyTestBase.restartMiniCluster: Shutting down cluster\n");
    TEST_UTIL.shutdownMiniHBaseCluster();

    LOG.info("\n\nManagedKeyTestBase.restartMiniCluster: Sleeping a bit\n");
    Thread.sleep(2000);

    LOG.info("\n\nManagedKeyTestBase.restartMiniCluster: Starting the cluster back up\n");
    TEST_UTIL.restartHBaseCluster(1);

    waitForMasterInitialization(tableNameToWaitFor);
  }

  private void waitForMasterInitialization(TableName tableNameToWaitFor) throws Exception {
    LOG.info(
      "\n\nManagedKeyTestBase.waitForMasterInitialization: Waiting for master initialization\n");
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());

    LOG.info(
      "\n\nManagedKeyTestBase.waitForMasterInitialization: Waiting for regions to be assigned\n");
    TEST_UTIL.waitUntilAllRegionsAssigned(tableNameToWaitFor);
    LOG.info("\n\nManagedKeyTestBase.waitForMasterInitialization: Regions assigned\n");
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("\n\nManagedKeyTestBase.tearDown: Shutting down cluster\n");
    TEST_UTIL.shutdownMiniCluster();
    LOG.info("\n\nManagedKeyTestBase.tearDown: Cluster successfully shut down\n");
    // Clear the provider cache to avoid test interference
    Encryption.clearKeyProviderCache();
  }

  protected Class<? extends ManagedKeyProvider> getKeyProviderClass() {
    return MockManagedKeyProvider.class;
  }

  protected boolean isWithKeyManagement() {
    return true;
  }

  protected boolean isWithMiniClusterStart() {
    return true;
  }

  protected TableName getSystemTableNameToWaitFor() {
    return KeymetaTableAccessor.KEY_META_TABLE_NAME;
  }

  /**
   * Useful hook to enable setting a breakpoint while debugging ruby tests, just log a message and
   * you can even have a conditional breakpoint.
   */
  protected void logMessage(String msg) {
    LOG.info(msg);
  }
}
