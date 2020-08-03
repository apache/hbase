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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to support persistent storefile tracking
 */
@InterfaceAudience.Private
public class StorefileTrackingUtils {

  private static Logger LOG = LoggerFactory.getLogger(StorefileTrackingUtils.class);
  public static final long SLEEP_DELTA_MS = TimeUnit.MILLISECONDS.toMillis(100);

  public static boolean isStorefileTrackingPersistEnabled(Configuration conf) {
    boolean isStoreTrackingPersistEnabled =
      conf.getBoolean(HConstants.STOREFILE_TRACKING_PERSIST_ENABLED,
        HConstants.DEFAULT_STOREFILE_TRACKING_PERSIST_ENABLED);
    boolean isPersistedStoreEngineSet =
      conf.get(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName())
        .equals(PersistedStoreEngine.class.getName());
    boolean isFeatureEnabled = isStoreTrackingPersistEnabled && isPersistedStoreEngineSet;
    if (isStoreTrackingPersistEnabled ^ isPersistedStoreEngineSet) {
      // check if both configuration are correct.
      String errorMessage = String.format("please set %s to true and set store engine key %s to %s "
          + "to enable persist storefile tracking",
        HConstants.STOREFILE_TRACKING_PERSIST_ENABLED,
        StoreEngine.STORE_ENGINE_CLASS_KEY, PersistedStoreEngine.class.getName());
      throw new IllegalArgumentException(errorMessage);
    }
    return isFeatureEnabled;
  }

  /**
   * if storefile tracking feature is configured, Initialize hbase:storefile table and wait for it
   * to be online. Otherwise, look for hbase:storefile table and remove it
   *
   * @param masterServices
   * @throws IOException
   */
  public static void init(MasterServices masterServices) throws IOException {
    createStorefileTable(masterServices);
    waitForStoreFileTableOnline(masterServices);
  }

  /**
   * Cleans up all storefile related state on the cluster. disable and delete hbase:storefile
   * if found
   * @param masterServices {@link MasterServices}
   * @throws IOException
   */
  private static void cleanup(MasterServices masterServices) throws IOException {
    try {
      masterServices.getConnection().getAdmin().disableTable(TableName.STOREFILE_TABLE_NAME);
      masterServices.getConnection().getAdmin().deleteTable(TableName.STOREFILE_TABLE_NAME);
    } catch (IOException ex) {
      final String message = "Failed disable and deleting table " +
        TableName.STOREFILE_TABLE_NAME.getNameAsString();
      LOG.error(message);
      throw new IOException(message, ex);
    }
  }

  public static StoreFilePathAccessor createStoreFilePathAccessor(Configuration conf,
    Connection connection) {
    return new HTableStoreFilePathAccessor(conf, connection);
  }

  public static List<Path> convertStoreFilesToPaths(Collection<HStoreFile> storeFiles) {
    return storeFiles.stream().map(HStoreFile::getPath).collect(Collectors.toList());
  }

  private static void createStorefileTable(MasterServices masterServices)
    throws IOException {
    if (MetaTableAccessor.getTableState(masterServices.getConnection(),
      TableName.STOREFILE_TABLE_NAME) == null) {
      LOG.info("{} table not found. Creating...", TableName.STOREFILE_TABLE_NAME);
      masterServices.createSystemTable(HTableStoreFilePathAccessor.STOREFILE_TABLE_DESC);
    }
  }

  private static void waitForStoreFileTableOnline(MasterServices masterServices)
    throws IOException {
    try {
      long startTime = EnvironmentEdgeManager.currentTime();
      long timeout = masterServices.getConfiguration()
        .getLong(HConstants.STOREFILE_TRACKING_INIT_TIMEOUT,
          HConstants.DEFAULT_STOREFILE_TRACKING_INIT_TIMEOUT);
      while (!isStoreFileTableAssignedAndEnabled(masterServices)) {
        if (EnvironmentEdgeManager.currentTime() - startTime + SLEEP_DELTA_MS > timeout) {
          throw new IOException("Time out " + timeout + " ms waiting for hbase:storefile table to "
            + "be assigned and enabled: " + masterServices.getTableStateManager()
            .getTableState(TableName.STOREFILE_TABLE_NAME));
        }
        Thread.sleep(SLEEP_DELTA_MS);
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted when wait for " + TableName.STOREFILE_TABLE_NAME
        + " to be assigned and enabled", e);
    }
  }

  public static boolean isStoreFileTableAssignedAndEnabled(MasterServices masterServices)
    throws IOException {
    return masterServices.getAssignmentManager().getRegionStates()
      .hasTableRegionStates(TableName.STOREFILE_TABLE_NAME) && masterServices
      .getTableStateManager().getTableState(TableName.STOREFILE_TABLE_NAME).isEnabled();
  }

  static String getFamilyFromKey(String key, String tableName, String regionName,
    String separator) {
    assert key.startsWith(regionName) : "Unexpected suffix for row key from hbase:storefile "
      + "table";
    int startIndex = regionName.length() + separator.length();
    int endIndex = key.lastIndexOf(separator + tableName);
    return key.substring(startIndex, endIndex);
  }
}
