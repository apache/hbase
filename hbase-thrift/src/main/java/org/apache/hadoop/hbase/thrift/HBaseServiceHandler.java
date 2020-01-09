/**
 *
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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * abstract class for HBase handler
 * providing a Connection cache and get table/admin method
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public abstract class HBaseServiceHandler {
  public static final String CLEANUP_INTERVAL = "hbase.thrift.connection.cleanup-interval";
  public static final String MAX_IDLETIME = "hbase.thrift.connection.max-idletime";

  protected Configuration conf;

  protected final ConnectionCache connectionCache;

  public HBaseServiceHandler(final Configuration c,
      final UserProvider userProvider) throws IOException {
    this.conf = c;
    int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
    int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
    connectionCache = new ConnectionCache(
        conf, userProvider, cleanInterval, maxIdleTime);
  }

  protected ThriftMetrics metrics = null;

  public void initMetrics(ThriftMetrics metrics) {
    this.metrics = metrics;
  }

  public void setEffectiveUser(String effectiveUser) {
    connectionCache.setEffectiveUser(effectiveUser);
  }

  /**
   * Obtain HBaseAdmin. Creates the instance if it is not already created.
   */
  protected Admin getAdmin() throws IOException {
    return connectionCache.getAdmin();
  }

  /**
   * Creates and returns a Table instance from a given table name.
   *
   * @param tableName
   *          name of table
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
