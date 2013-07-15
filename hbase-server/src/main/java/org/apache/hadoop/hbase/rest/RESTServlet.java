/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionWrapper;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
@InterfaceAudience.Private
public class RESTServlet implements Constants {
  private static RESTServlet INSTANCE;
  private final Configuration conf;
  private final HTablePool pool;
  private final MetricsREST metrics = new MetricsREST();
  private final HBaseAdmin admin;
  private final UserGroupInformation ugi;

  /**
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance() throws IOException {
    assert(INSTANCE != null);
    return INSTANCE;
  }

  /**
   * @param conf Existing configuration to use in rest servlet
   * @return the RESTServlet singleton instance
   * @throws IOException
   */
  public synchronized static RESTServlet getInstance(Configuration conf)
  throws IOException {
    return getInstance(conf, null);
  }

  public synchronized static RESTServlet getInstance(Configuration conf,
      UserGroupInformation ugi) throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new RESTServlet(conf, ugi);
    }
    return INSTANCE;
  }

  public synchronized static void stop() {
    if (INSTANCE != null)  INSTANCE = null;
  }

  /**
   * Constructor with existing configuration
   * @param conf existing configuration
   * @throws IOException
   */
  RESTServlet(final Configuration conf,
      final UserGroupInformation ugi) throws IOException {
    this.conf = conf;
    this.ugi = ugi;
    int maxSize = conf.getInt("hbase.rest.htablepool.size", 10);
    if (ugi == null) {
      pool = new HTablePool(conf, maxSize);
      admin = new HBaseAdmin(conf);
    } else {
      admin = new HBaseAdmin(new HConnectionWrapper(ugi,
        HConnectionManager.getConnection(new Configuration(conf))));

      pool = new HTablePool(conf, maxSize) {
        /**
         * A HTablePool adapter. It makes sure the real user is
         * always used in creating any table so that the HConnection
         * is not any proxy user in case impersonation with
         * RESTServletContainer.
         */
        @Override
        protected HTableInterface createHTable(final String tableName) {
          return ugi.doAs(new PrivilegedAction<HTableInterface>() {
            @Override
            public HTableInterface run() {
              return callCreateHTable(tableName);
             }
           });
          
        }

        /**
         * A helper method used to call super.createHTable.
         */
        HTableInterface callCreateHTable(final String tableName) {
          return super.createHTable(tableName);
        }
      };
    }
  }

  HBaseAdmin getAdmin() {
    return admin;
  }

  HTablePool getTablePool() {
    return pool;
  }

  Configuration getConfiguration() {
    return conf;
  }

  MetricsREST getMetrics() {
    return metrics;
  }

  /**
   * Helper method to determine if server should
   * only respond to GET HTTP method requests.
   * @return boolean for server read-only state
   */
  boolean isReadOnly() {
    return getConfiguration().getBoolean("hbase.rest.readonly", false);
  }

  UserGroupInformation getUser() {
    return ugi;
  }
}
