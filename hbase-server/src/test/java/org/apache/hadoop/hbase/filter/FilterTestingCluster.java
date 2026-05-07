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
package org.apache.hadoop.hbase.filter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;

/**
 * By using this class as the super class of a set of tests you will have a HBase testing cluster
 * available that is very suitable for writing tests for scanning and filtering against.
 */
public abstract class FilterTestingCluster {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Admin admin = null;
  private static List<TableName> createdTables = new ArrayList<>();

  protected static void createTable(TableName tableName, String columnFamilyName) {
    assertNotNull(admin, "HBaseAdmin is not initialized successfully.");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(columnFamilyName))).build();

    try {
      admin.createTable(tableDescriptor);
      createdTables.add(tableName);
      assertTrue(admin.tableExists(tableName), "Fail to create the table");
    } catch (IOException e) {
      assertNull(e, "Exception found while creating table");
    }
  }

  protected static Table openTable(TableName tableName) throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    assertTrue(admin.tableExists(tableName), "Fail to create the table");
    return table;
  }

  private static void deleteTables() {
    if (admin != null) {
      for (TableName tableName : createdTables) {
        try {
          if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
          }
        } catch (IOException e) {
          assertNull(e, "Exception found deleting the table");
        }
      }
    }
  }

  private static void initialize(Configuration conf) {
    conf = HBaseConfiguration.create(conf);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    try {
      admin = TEST_UTIL.getAdmin();
    } catch (MasterNotRunningException e) {
      assertNull(e, "Master is not running");
    } catch (ZooKeeperConnectionException e) {
      assertNull(e, "Cannot connect to ZooKeeper");
    } catch (IOException e) {
      assertNull(e, "IOException");
    }
  }

  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    deleteTables();
    TEST_UTIL.shutdownMiniCluster();
  }
}
