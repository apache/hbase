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

package org.apache.hadoop.hbase.quotas;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.StringUtils;

/**
 * Scanner to iterate over the quota settings.
 */
@InterfaceAudience.Public
public class QuotaRetriever implements Closeable, Iterable<QuotaSettings> {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaRetriever.class);

  private final Queue<QuotaSettings> cache = new LinkedList<>();
  private ResultScanner scanner;
  /**
   * Connection to use.
   * Could pass one in and have this class use it but this class wants to be standalone.
   */
  private Connection connection;
  private Table table;

  /**
   * Should QutoaRetriever manage the state of the connection, or leave it be.
   */
  private boolean isManagedConnection = false;

  QuotaRetriever() {
  }

  void init(final Configuration conf, final Scan scan) throws IOException {
    // Set this before creating the connection and passing it down to make sure
    // it's cleaned up if we fail to construct the Scanner.
    this.isManagedConnection = true;
    init(ConnectionFactory.createConnection(conf), scan);
  }

  void init(final Connection conn, final Scan scan) throws IOException {
    this.connection = Objects.requireNonNull(conn);
    this.table = this.connection.getTable(QuotaTableUtil.QUOTA_TABLE_NAME);
    try {
      scanner = table.getScanner(scan);
    } catch (IOException e) {
      try {
        close();
      } catch (IOException ioe) {
        LOG.warn("Failed getting scanner and then failed close on cleanup", e);
      }
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (this.table != null) {
      this.table.close();
      this.table = null;
    }
    // Null out the connection on close() even if we didn't explicitly close it
    // to maintain typical semantics.
    if (isManagedConnection) {
      if (this.connection != null) {
        this.connection.close();
      }
    }
    this.connection = null;
  }

  public QuotaSettings next() throws IOException {
    if (cache.isEmpty()) {
      Result result = scanner.next();
      // Skip exceedThrottleQuota row key because this is not a QuotaSettings
      if (result != null
          && Bytes.equals(result.getRow(), QuotaTableUtil.getExceedThrottleQuotaRowKey())) {
        result = scanner.next();
      }
      if (result == null) {
        return null;
      }
      QuotaTableUtil.parseResultToCollection(result, cache);
    }
    return cache.poll();
  }

  @Override
  public Iterator<QuotaSettings> iterator() {
    return new Iter();
  }

  private class Iter implements Iterator<QuotaSettings> {
    QuotaSettings cache;

    public Iter() {
      try {
        cache = QuotaRetriever.this.next();
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

    @Override
    public boolean hasNext() {
      return cache != null;
    }

    @Override
    public QuotaSettings next() {
      QuotaSettings result = cache;
      try {
        cache = QuotaRetriever.this.next();
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
      return result;
    }

    @Override
    public void remove() {
      throw new RuntimeException("remove() not supported");
    }
  }

  /**
   * Open a QuotaRetriever with no filter, all the quota settings will be returned.
   * @param conf Configuration object to use.
   * @return the QuotaRetriever
   * @throws IOException if a remote or network exception occurs
   */
  public static QuotaRetriever open(final Configuration conf) throws IOException {
    return open(conf, null);
  }

  /**
   * Open a QuotaRetriever with the specified filter.
   * @param conf Configuration object to use.
   * @param filter the QuotaFilter
   * @return the QuotaRetriever
   * @throws IOException if a remote or network exception occurs
   */
  public static QuotaRetriever open(final Configuration conf, final QuotaFilter filter)
      throws IOException {
    Scan scan = QuotaTableUtil.makeScan(filter);
    QuotaRetriever scanner = new QuotaRetriever();
    scanner.init(conf, scan);
    return scanner;
  }
}
