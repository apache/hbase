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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.RemovalCause;

/**
 * abstract class for HBase handler providing a Connection cache and get table/admin method
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public abstract class HBaseServiceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseServiceHandler.class);

  public static final String CLEANUP_INTERVAL = "hbase.thrift.connection.cleanup-interval";
  public static final String MAX_IDLETIME = "hbase.thrift.connection.max-idletime";

  protected Configuration conf;

  protected final ConnectionCache connectionCache;

  protected static final class ResultScannerWrapper {
    public final ResultScanner scanner;
    public final boolean sortColumns;
    public final String owner;

    public ResultScannerWrapper(ResultScanner scanner, boolean sortColumns, String owner) {
      this.scanner = scanner;
      this.sortColumns = sortColumns;
      this.owner = owner;
    }
  }

  private final AtomicInteger nextScannerId = new AtomicInteger(0);
  private final Cache<Integer, ResultScannerWrapper> scannerMap;
  private final KeyLocker<Integer> removeScannerLock = new KeyLocker<>();

  public HBaseServiceHandler(final Configuration c, final UserProvider userProvider)
    throws IOException {
    this.conf = c;
    int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
    int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
    connectionCache = new ConnectionCache(conf, userProvider, cleanInterval, maxIdleTime);
    long cacheTimeout = conf.getLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    scannerMap = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS)
      .removalListener(notification -> {
        // do not close the scanner if it is removed manually, we will either add it back or close
        // it manually.
        if (notification.getCause() != RemovalCause.EXPLICIT) {
          ((ResultScannerWrapper) notification.getValue()).scanner.close();
        }
      }).build();
  }

  protected ThriftMetrics metrics = null;

  public void initMetrics(ThriftMetrics metrics) {
    this.metrics = metrics;
  }

  public void setEffectiveUser(String effectiveUser) {
    connectionCache.setEffectiveUser(effectiveUser);
  }

  /**
   * Assigns a unique ID to the scanner and adds the mapping to an internal HashMap.
   * @param scanner to add
   * @return Id for this Scanner
   */
  protected int addScanner(ResultScanner scanner, boolean sortColumns) {
    int id = nextScannerId.getAndIncrement();
    ResultScannerWrapper wrapper =
      new ResultScannerWrapper(scanner, sortColumns, connectionCache.getEffectiveUser());
    scannerMap.put(id, wrapper);
    return id;
  }

  /**
   * Add the given scanner back to scanner map.
   * <p>
   * When scanning, we need to remove the scanner from scanner map to prevent expiration during
   * scanning.
   */
  protected void addScannerBack(int id, ResultScannerWrapper wrapper) {
    scannerMap.put(id, wrapper);
  }

  /**
   * Removes the scanner associated with the specified ID from the internal HashMap.
   * @param id of the Scanner to remove
   * @throws AccessDeniedException if the scanner is not belong to the current user
   */
  protected ResultScannerWrapper removeScanner(int id) throws IOException {
    Lock lock = removeScannerLock.acquireLock(id);
    try {
      ResultScannerWrapper wrapper = scannerMap.getIfPresent(id);
      if (wrapper != null && !Objects.equals(connectionCache.getEffectiveUser(), wrapper.owner)) {
        LOG.warn("User {} is trying to access scanner id = {} where owner = {}",
          connectionCache.getEffectiveUser(), id, wrapper.owner);
        throw new AccessDeniedException(
          "User " + connectionCache.getEffectiveUser() + " is not allowed to access scanner " + id);
      }
      scannerMap.invalidate(id);
      return wrapper;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Obtain HBaseAdmin. Creates the instance if it is not already created.
   */
  protected Admin getAdmin() throws IOException {
    return connectionCache.getAdmin();
  }

  /**
   * Creates and returns a Table instance from a given table name. name of table
   * @return Table object
   * @throws IOException if getting the table fails
   */
  protected Table getTable(final byte[] tableName) throws IOException {
    String table = Bytes.toString(tableName);
    return connectionCache.getTable(table);
  }

  protected Table getTable(final ByteBuffer tableName) throws IOException {
    return getTable(Bytes.getBytes(tableName));
  }
}
