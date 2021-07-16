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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class TestCompactionServerBase {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Configuration CONF = TEST_UTIL.getConfiguration();
  protected static HMaster MASTER;
  protected static HCompactionServer COMPACTION_SERVER;
  protected static ServerName COMPACTION_SERVER_NAME;
  protected static TableName TABLENAME = TableName.valueOf("t");
  protected static String FAMILY = "C";
  protected static String COL ="c0";

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numCompactionServers(1).build());
    TEST_UTIL.getAdmin().switchCompactionOffload(true);
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    COMPACTION_SERVER = TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0)
      .getCompactionServer();
    COMPACTION_SERVER_NAME = COMPACTION_SERVER.getServerName();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TABLENAME).setCompactionOffloadEnabled(true).build();
    TEST_UTIL.createTable(tableDescriptor, Bytes.toByteArrays(FAMILY),
      TEST_UTIL.getConfiguration());
    TEST_UTIL.waitTableAvailable(TABLENAME);
    COMPACTION_SERVER.requestCount.reset();
  }

  @After
  public void after() throws IOException {
    TEST_UTIL.deleteTableIfAny(TABLENAME);
  }

  void doPutRecord(int start, int end, boolean flush) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL), Bytes.toBytes(i));
      h.put(p);
      if (i % 100 == 0 && flush) {
        TEST_UTIL.flush(TABLENAME);
      }
    }
    h.close();
  }

  void doFillRecord(int start, int end, byte[] value) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL), value);
      h.put(p);
    }
    h.close();
  }

  void verifyRecord(int start, int end, boolean exist) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Get get = new Get(Bytes.toBytes(i));
      Result r = h.get(get);
      if (exist) {
        assertArrayEquals(Bytes.toBytes(i), r.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL)));
      } else {
        assertNull(r.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL)));
      }
    }
    h.close();
  }
}
