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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestScanOrGetWithReplicationFromClient {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScanOrGetWithReplicationFromClient.class);

  @Rule
  public TestName name = new TestName();

  private ExpectedException exception = ExpectedException.none();

  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String ROW = "r1";
  private static final byte[] FAMILY = Bytes.toBytes("info");
  private static final int REGION_REPLICATION_COUNT = 2;
  // region replica id starts from 0
  private static final int NON_EXISTING_REGION_REPLICA_ID = REGION_REPLICATION_COUNT;
  private static Connection connection;
  private static Admin admin;
  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    connection = UTIL.getConnection();
    admin = UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.cleanup(null, admin);
    IOUtils.cleanup(null, connection);
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build();
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnFamilyDescriptor)
        .setRegionReplication(REGION_REPLICATION_COUNT).build();
    admin.createTable(tableDescriptor);
    UTIL.waitTableAvailable(tableName);
    assertTrue(admin.tableExists(tableName));
    assertEquals(REGION_REPLICATION_COUNT, tableDescriptor.getRegionReplication());

    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    assertEquals(REGION_REPLICATION_COUNT, regions.size());

    Table table = connection.getTable(tableName);
    Put put = new Put(Bytes.toBytes(ROW)).addColumn(FAMILY, Bytes.toBytes("q"),
      Bytes.toBytes("test_value"));
    table.put(put);
    admin.flush(tableName);

    Scan scan = new Scan();
    ResultScanner rs = table.getScanner(scan);
    rs.forEach(r -> assertEquals(ROW, Bytes.toString(r.getRow())));
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(tableName);
  }

  @Test
  public void testScanMetaWithRegionReplicaId() throws IOException {
    Table metaTable = connection.getTable(TableName.META_TABLE_NAME);
    Scan scan = new Scan();
    scan.setFilter(new PrefixFilter(tableName.getName()));
    scan.setReplicaId(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    scan.setConsistency(Consistency.TIMELINE);
    ResultScanner rs = metaTable.getScanner(scan);
    rs.forEach(r -> assertTrue(Bytes.toString(r.getRow()).contains(tableName.getNameAsString())));
  }

  @Test
  public void testScanMetaWithNonExistingRegionReplicaId() throws IOException {
    Table metaTable = connection.getTable(TableName.META_TABLE_NAME);
    Scan scan = new Scan();
    scan.setReplicaId(NON_EXISTING_REGION_REPLICA_ID);
    scan.setConsistency(Consistency.TIMELINE);
    exception.expect(DoNotRetryIOException.class);
    ResultScanner rs = metaTable.getScanner(scan);
    try {
      rs.forEach(r -> Bytes.toString(r.getRow()));
    } catch (Exception e) {
      Throwable throwable = e.getCause();
      assertTrue(throwable instanceof DoNotRetryIOException);
      String message = "The specified region replica id " + NON_EXISTING_REGION_REPLICA_ID
        + " does not exist, the REGION_REPLICATION of this table "
        + TableName.META_TABLE_NAME.getNameAsString() + " is "
        + TableDescriptorBuilder.DEFAULT_REGION_REPLICATION + ", "
        + "this means that the maximum region replica id you can specify is "
        + (TableDescriptorBuilder.DEFAULT_REGION_REPLICATION - 1) + ".";
      assertEquals(message, throwable.getMessage());
    }
  }

  @Test
  public void testScanTableWithRegionReplicaId() throws IOException {
    Table table = connection.getTable(tableName);
    Scan scan = new Scan();
    scan.setReplicaId(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    scan.setConsistency(Consistency.TIMELINE);
    ResultScanner rs = table.getScanner(scan);
    rs.forEach(r -> assertEquals(ROW, Bytes.toString(r.getRow())));
  }

  @Test
  public void testScanTableWithNonExistingRegionReplicaId() throws IOException {
    Table table = connection.getTable(tableName);
    Scan scan = new Scan();
    scan.setReplicaId(NON_EXISTING_REGION_REPLICA_ID);
    scan.setConsistency(Consistency.TIMELINE);
    exception.expect(DoNotRetryIOException.class);
    ResultScanner rs = table.getScanner(scan);
    try {
      rs.forEach(r -> Bytes.toString(r.getRow()));
    } catch (Exception e) {
      Throwable throwable = e.getCause();
      assertTrue(throwable instanceof DoNotRetryIOException);
      String message = "The specified region replica id " + NON_EXISTING_REGION_REPLICA_ID
        + " does not exist, the REGION_REPLICATION of this table " + tableName.getNameAsString()
        + " is " + REGION_REPLICATION_COUNT + ", "
        + "this means that the maximum region replica id you can specify is "
        + (REGION_REPLICATION_COUNT - 1) + ".";
      assertEquals(message, throwable.getMessage());
    }
  }

  @Test
  public void testGetTableWithRegionReplicaId() throws IOException {
    Table table = connection.getTable(tableName);
    Get get = new Get(Bytes.toBytes(ROW)).setConsistency(Consistency.TIMELINE)
      .setReplicaId(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    Result result = table.get(get);
    assertEquals(ROW, Bytes.toString(result.getRow()));
    String value = Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("q")));
    assertEquals("test_value", value);
  }

  @Test
  public void testGetTableWithNonExistingRegionReplicaId() throws IOException {
    Table table = connection.getTable(tableName);
    Get get = new Get(Bytes.toBytes(ROW)).setConsistency(Consistency.TIMELINE)
      .setReplicaId(NON_EXISTING_REGION_REPLICA_ID);
    try {
      Result result = table.get(get);
      result.getValue(FAMILY, Bytes.toBytes("q"));
    } catch (Exception e) {
      assertTrue(e instanceof DoNotRetryIOException);
      String message = "The specified region replica id " + NON_EXISTING_REGION_REPLICA_ID
        + " does not exist, the REGION_REPLICATION of this table " + tableName.getNameAsString()
        + " is " + REGION_REPLICATION_COUNT + ", "
        + "this means that the maximum region replica id you can specify is "
        + (REGION_REPLICATION_COUNT - 1) + ".";
      assertEquals(message, e.getMessage());
    }
  }
}
