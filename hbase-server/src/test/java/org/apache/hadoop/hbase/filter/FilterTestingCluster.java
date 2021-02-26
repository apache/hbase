/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * By using this class as the super class of a set of tests you will have a HBase testing
 * cluster available that is very suitable for writing tests for scanning and filtering against.
 */
@Category({FilterTests.class, MediumTests.class})
public class FilterTestingCluster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Admin admin = null;
  private static List<TableName> createdTables = new ArrayList<>();

  protected static void createTable(TableName tableName, String columnFamilyName) {
    assertNotNull("HBaseAdmin is not initialized successfully.", admin);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDef = new HColumnDescriptor(Bytes.toBytes(columnFamilyName));
    desc.addFamily(colDef);

    try {
      admin.createTable(desc);
      createdTables.add(tableName);
      assertTrue("Fail to create the table", admin.tableExists(tableName));
    } catch (IOException e) {
      assertNull("Exception found while creating table", e);
    }
  }

  protected static Table openTable(TableName tableName) throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    assertTrue("Fail to create the table", admin.tableExists(tableName));
    return table;
  }

  private static void deleteTables() {
    if (admin != null) {
      for (TableName tableName: createdTables){
        try {
          if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
          }
        } catch (IOException e) {
          assertNull("Exception found deleting the table", e);
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
      assertNull("Master is not running", e);
    } catch (ZooKeeperConnectionException e) {
      assertNull("Cannot connect to ZooKeeper", e);
    } catch (IOException e) {
      assertNull("IOException", e);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteTables();
    TEST_UTIL.shutdownMiniCluster();
  }
}
