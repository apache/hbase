package org.apache.hadoop.hbase.thrift.swift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;


import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableAsyncInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSimpleOperations {
  private static final Log LOG = LogFactory.getLog(TestSimpleOperations.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int SLAVES = 1;

  static final String ROW_PREFIX = "row";
  static int testCount = 0;
  static byte[] TABLE = Bytes.toBytes("testTable" + testCount);
  static final byte[] FAMILY = Bytes.toBytes("family");
  static final byte[][] FAMILIES = new byte[][] { FAMILY };
  static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  static final String VALUE_PREFIX = "value";
  static final byte[] FAMILY1 = Bytes.toBytes("family1");
  static final byte[] FAMILY2 = Bytes.toBytes("family2");
  static final byte[][] FAMILIES1 = new byte[][] { FAMILY1, FAMILY2 };
  static final byte[] QUALIFIER1 = Bytes.toBytes("q1");
  static final byte[] QUALIFIER2 = Bytes.toBytes("q2");

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanUp() throws IOException {
    TABLE = Bytes.toBytes("testTable" + ++testCount);
  }

  /**
   * Test if doing a simple put using Hadoop RPC, followed by doing a simple get
   * on that row (using thrift, this time), works or not.
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSimpleGetUsingThrift() throws IOException {
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);

    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    Result result1 = ht.get(g);

    LOG.debug("Found the result : " + result1);
    assertTrue(Bytes.equals(result1.getValue(FAMILY, null), value));
  }

  /**
   * Test {@link HRegionInterface#put(byte[], List)}
   *
   * Doing a batch mutate with 100 puts inside, then doing a get for each of the
   * puts
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testPutMultiViaThrift() throws IOException {
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] r = Bytes.toBytes("r" + i);
      Put put = new Put(r);
      byte[] value = Bytes.toBytes("test-value" + i);
      put.add(FAMILY, null, value);
      mutations.add(put);
    }

    ht.batchMutate(mutations);
    for (int i = 0; i < 100; i++) {
      Get g = new Get.Builder(Bytes.toBytes("r" + i)).addFamily(FAMILY).create();
      Result result1 = ht.get(g);
      assertTrue(Bytes.equals(result1.getValue(FAMILY, null), Bytes.toBytes("test-value"+i)));
    }
  }

  /**
   * Test if doing a simple put, followed by doing a simple asynchronous
   * get on that row (using thrift), works or not.
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testSimpleAsynchronousGet() throws Exception {
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");

    HTableAsyncInterface ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);

    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    ListenableFuture<Result> future = ht.getAsync(g);
    Result result1 = future.get();

    assertTrue(Bytes.equals(result1.getValue(FAMILY, null), value));
  }

  /**
   * Test if doing a simple put using thrift, followed by doing a simple get
   * on that row (using Hadoop RPC, this time), works or not.
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSimplePutUsingThrift() throws IOException {
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");
    Result result;

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);

    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    result = ht.get(g);

    LOG.debug("Found the result : " + result);
    assertTrue(Bytes.equals(result.getValue(FAMILY, null), value));
  }

  /**
   * Test to verify that multiputs are sent fine from end-to-end, when sent
   * through Thrift.
   *
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testMultiPutUsingThrift() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES);
    List<Put> puts = createDummyPuts(10);
    // The list of puts is batched as a MultiPut and sent across.
    table.put(puts);

    // Now verify if all the puts made it through, use the Hadoop RPC.
    verifyDummyMultiPut(table, 10);
  }

  List<Put> createDummyPuts(int n) {
    List<Put> putList = new ArrayList<Put>();
    for (int i = 0; i < n; i++) {
      Put p = new Put(Bytes.toBytes(ROW_PREFIX + i),
        System.currentTimeMillis());
      p.add(FAMILY, QUALIFIER, Bytes.toBytes(VALUE_PREFIX + i));
      putList.add(p);
    }
    return putList;
  }

  void verifyDummyMultiPut(HTable table, int n)
    throws IOException {
    for (int i = 0; i < n; i++) {
      Get g = new Get(Bytes.toBytes(ROW_PREFIX + i));
      Result r = table.get(g);
      NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(FAMILY);
      assertTrue(Arrays.equals(familyMap.get(QUALIFIER),
        Bytes.toBytes(VALUE_PREFIX + i)));
    }
  }

  /**
   * Verify that we can do a Delete operation using Delete. We achieve this
   * by doing a Put, followed by a Get to verify that it went through. Then
   * we do a Delete followed by a Get to verify that the Delete went through.
   *
   * Only the Delete is done using Thrift.
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testDeleteUsingThrift() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES);
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");
    Result result;

    // Do a Put
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    table.put(put);

    // Do a Get to check it went through
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    result = table.get(g);
    assertTrue(Bytes.equals(result.getValue(FAMILY, null), value));

    // Now do a Delete using Thrift.
    Delete del = new Delete(r1);
    table.delete(del);

    // Now do the same get again and check that the delete works.
    result = table.get(g);
    assertTrue(result.isEmpty());
  }

  /**
   * Verify that we can do a Delete operation using asynchronous Delete. We achieve
   * this by doing a Put, followed by a Get to verify that it went through. Then
   * we do a Delete followed by a Get to verify that the Delete went through.
   *
   * Only the Delete is done using Thrift.
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testSimpleAsynchronousDelete() throws Exception {
    HTableAsyncInterface table = TEST_UTIL.createTable(TABLE, FAMILIES);
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");
    Result result;

    // Do a Put
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    table.put(put);

    // Do a Get to check it went through
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    result = table.get(g);
    assertTrue(Bytes.equals(result.getValue(FAMILY, null), value));

    // Now do a Delete using Thrift.
    Delete del = new Delete(r1);
    ListenableFuture<Void> future = table.deleteAsync(del);
    future.get();

    // Now do the same get again and check that the delete works.
    result = table.get(g);
    assertTrue(result.isEmpty());
  }

  /**
   * Check if the Delete thrift annotation lets you do all that the
   * earlier delete would let you.
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testDeleteThriftAnnotationIsSufficient() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES1);
    byte[] r1 = Bytes.toBytes("r1");
    byte[] r2 = Bytes.toBytes("r2");
    byte[] v1 = Bytes.toBytes("v1");
    byte[] v2 = Bytes.toBytes("v2");

    // Do a Put
    Put put1 = new Put(r1);
    // Adding two KVs, <FAMILY1:QUALIFIER1, v1> and <FAMILY2:QUALIFIER2, v2>
    put1.add(FAMILY1, QUALIFIER1, v1);
    put1.add(FAMILY2, QUALIFIER2, v2);
    table.put(put1);

    // Now do a Delete using Thrift.
    Delete del1 = new Delete(r1);
    // We delete only FAMILY2. The first KV should survive.
    del1.deleteFamily(FAMILY2);
    // Use thrift while doing this.
    table.delete(del1);

    Get g1 = new Get(r1);
    Result res1 = table.get(g1);
    // We should get only one Key Value, since we deleted the other CF.
    assertEquals(1, res1.list().size());

    // Make sure that we got the right KV: <FAMILY1:QUALIFIER1, v1>
    KeyValue kv1 = res1.list().get(0);
    assertTrue(Bytes.equals(kv1.getFamily(), FAMILY1));
    assertTrue(Bytes.equals(kv1.getQualifier(), QUALIFIER1));
    assertTrue(Bytes.equals(kv1.getValue(), v1));

    Put put2 = new Put(r2);
    put2.add(FAMILY1, QUALIFIER1, v1);
    put2.add(FAMILY1, QUALIFIER2, v2);
    table.put(put2);

    Delete del2 = new Delete(r2);
    // Now try if we can delete columns.
    del2.deleteColumn(FAMILY1, QUALIFIER1);
    // Use thrift to do this.
    table.delete(del2);

    Get g2 = new Get(r2);
    // Do a standard get.
    Result res2 = table.get(g2);

    // Since we deleted <FAMILY1:QUALIFIER1, v1>, we expect only
    // <FAMILY1:QUALIFIER2, v2>
    KeyValue kv2 = res2.list().get(0);
    // Check everything is the same.
    assertTrue(Bytes.equals(kv2.getFamily(), FAMILY1));
    assertTrue(Bytes.equals(kv2.getQualifier(), QUALIFIER2));
    assertTrue(Bytes.equals(kv2.getValue(), v2));
  }

  /**
   * Test if we can do a batch of deletes using Thrift. Do this by making a
   * bunch of puts and gets (to verify that the puts went through), and then
   * doing a batch of deletes for the puts, followed by gets (to verify that
   * the deletes went through).
   *
   * Only the Deletes use Thrift.
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testBatchOfDeletesUsingThrift() throws IOException {
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES);
    Result result;
    final int numPuts = 10;

    // Do a bunch of puts
    for (int i = 0; i < numPuts; i++) {
      Put put = new Put(Bytes.toBytes(ROW_PREFIX + i));
      put.add(FAMILY, null, Bytes.toBytes(VALUE_PREFIX + i));
      table.put(put);
    }

    // Do gets to verify that they happened
    for (int i = 0; i < numPuts; i++) {
      byte[] r = Bytes.toBytes(ROW_PREFIX + i);
      Get g = new Get.Builder(r).addFamily(FAMILY).create();
      result = table.get(g);
      assertTrue(Bytes.equals(result.getValue(FAMILY, null),
        Bytes.toBytes(VALUE_PREFIX + i)));
    }

    List<Delete> deletes = new LinkedList<Delete>();
    for (int i = 0; i < numPuts; i++) {
      deletes.add(new Delete(Bytes.toBytes(ROW_PREFIX + i)));
    }
    table.delete(deletes);

    for (int i = 0; i < numPuts; i++) {
      byte[] r = Bytes.toBytes(ROW_PREFIX + i);
      Get g = new Get.Builder(r).addFamily(FAMILY).create();
      result = table.get(g);
      assertTrue(result.isEmpty());
    }
  }

  /**
   * Check if the Put thrift annotation lets you do all that the
   * earlier Put would let you.
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testPutThriftAnnotationIsSufficient() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES1);
    byte[] r1 = Bytes.toBytes("r1");
    byte[] r2 = Bytes.toBytes("r2");
    byte[] v1 = Bytes.toBytes("v1");
    byte[] v2 = Bytes.toBytes("v2");

    // Do a Put using Thrift
    Put put1 = new Put(r1);
    // Adding two KVs, <FAMILY1:QUALIFIER1, v1> and <FAMILY2:QUALIFIER2, v2>
    put1.add(FAMILY1, QUALIFIER1, v1);
    put1.add(FAMILY2, QUALIFIER2, v2);
    table.put(put1);

    // Now do a Get using Hadoop RPC
    Get get1 = new Get(r1);
    Result result1 = table.get(get1);
    assertEquals(2, result1.list().size());
    KeyValue kv1 = result1.list().get(0);
    assertTrue(Bytes.equals(FAMILY1, kv1.getFamily()));
    assertTrue(Bytes.equals(QUALIFIER1, kv1.getQualifier()));
    assertTrue(Bytes.equals(v1, kv1.getValue()));

    KeyValue kv2 = result1.list().get(1);
    assertTrue(Bytes.equals(FAMILY2, kv2.getFamily()));
    assertTrue(Bytes.equals(QUALIFIER2, kv2.getQualifier()));
    assertTrue(Bytes.equals(v2, kv2.getValue()));

    // Do another put using Thrift, and using a different setter method.
    Put put2 = new Put(r2);
    KeyValue kv3 = new KeyValue(r2, FAMILY2, QUALIFIER1, v2);
    put2.add(kv3);
    table.put(put2);

    // Now do a Get using Hadoop RPC
    Get get2 = new Get(r2);
    Result result2 = table.get(get2);
    assertEquals(1, result2.list().size());
    KeyValue kv4 = result2.list().get(0);
    assertTrue(Bytes.equals(FAMILY2, kv4.getFamily()));
    assertTrue(Bytes.equals(QUALIFIER1, kv4.getQualifier()));
    assertTrue(Bytes.equals(v2, kv4.getValue()));
  }

  /**
   * Test getRowOrBefore test
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testGetRowOrBefore() throws IOException {
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testGetRowOrBefore"),
        FAMILY);
    byte[] value = Bytes.toBytes("value");
    Put p = new Put(Bytes.toBytes("bb"));
    p.add(FAMILY, null, value);
    table.put(p);
    p = new Put(Bytes.toBytes("ba"));
    p.add(FAMILY, null, value);
    table.put(p);
    table.flushCommits();

    Result r = table.getRowOrBefore(Bytes.toBytes("bad"), FAMILY);
    assertTrue(Bytes.equals(Bytes.toBytes("ba"), r.getRow()));
  }

  /**
   * Test getRowOrBeforeAsync
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testGetRowOrBeforeAsync() throws Exception {
    HTableAsyncInterface table = TEST_UTIL.createTable(Bytes.toBytes("testGetRowOrBeforeAsync"),
        FAMILY);
    byte[] value = Bytes.toBytes("value");
    Put p = new Put(Bytes.toBytes("bb"));
    p.add(FAMILY, null, value);
    table.put(p);
    p = new Put(Bytes.toBytes("ba"));
    p.add(FAMILY, null, value);
    table.put(p);
    table.flushCommits();

    Result r = table.getRowOrBeforeAsync(Bytes.toBytes("bad"), FAMILY).get();
    assertTrue(Bytes.equals(Bytes.toBytes("ba"), r.getRow()));
  }

  /**
   * Test if doing batchGet works via thrift
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSimpleMultiAction() throws IOException {
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");
    Result[] result;

    // do a put via Thrift
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);

    // do a batch get via Thrift
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    List<Get> gets = new ArrayList<>();
    gets.add(g);
    result = ht.batchGet(gets);
    LOG.debug("Found the result : " + result);
    assertTrue(result.length == 1);
    assertTrue(Bytes.equals(result[0].getValue(FAMILY, null), value));
  }

  /**
   * Test asynchronous put and batch get.
   *
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testBatchGetAsyncAction() throws Exception {
    byte[] r1 = Bytes.toBytes("r1");
    byte[] value = Bytes.toBytes("test-value");
    Result[] result;

    // do an asynchronous put
    HTableAsyncInterface ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put = new Put(r1);
    put.add(FAMILY, null, value);
    ht.put(put);

    // do a flush just for fun
    ht.flushCommitsAsync().get();

    // do an asynchronous batch get
    Get g = new Get.Builder(r1).addFamily(FAMILY).create();
    List<Get> gets = new ArrayList<>();
    gets.add(g);
    ListenableFuture<Result[]> future = ht.batchGetAsync(gets);
    result = future.get();
    LOG.debug("Found the result : " + result);
    assertTrue(result.length == 1);
    assertTrue(Bytes.equals(result[0].getValue(FAMILY, null), value));
  }

  /**
   * Do a batch mutate with 100 puts inside, then do a get for each of the
   * puts. Finally delete 90 rows and verify if scanner could get 10 rows.
   *
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testMultiAsyncActions() throws Exception {
    HTableAsyncInterface ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] r = Bytes.toBytes("r" + i);
      Put put = new Put(r);
      byte[] value = Bytes.toBytes("test-value" + i);
      put.add(FAMILY, null, value);
      mutations.add(put);
    }

    ht.batchMutateAsync(mutations).get();
    List<ListenableFuture<Result>> getFutures = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Get g = new Get.Builder(Bytes.toBytes("r" + i)).addFamily(FAMILY).create();
      getFutures.add(ht.getAsync(g));
    }
    for (int i = 0; i < 100; i++) {
      Result result = getFutures.get(i).get();
      assertTrue(Bytes.equals(result.getValue(FAMILY, null), Bytes.toBytes("test-value"+i)));
    }

    ArrayList<Mutation> deletes = new ArrayList<>();
    for (int i = 0; i < 90; i++) {
      Delete delete = new Delete(Bytes.toBytes("r" + i));
      delete.deleteFamily(FAMILY);
      deletes.add(delete);
    }
    ht.batchMutateAsync(deletes).get();
    Scan scan = new Scan.Builder().addFamily(FAMILY).create();
    ResultScanner scanner = ht.getScanner(scan);
    int numRows = 0;
    while (scanner.next() != null) {
      ++numRows;
    }
    assertEquals(numRows, 10);
  }

  /**
   * Lock a row and unlock it.
   *
   * @throws Exception
   */
  @Test(timeout=180000)
  public void testRowLock() throws Exception {
    HTableAsyncInterface ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    byte[] row = Bytes.toBytes("test-row");
    Put put = new Put(row);
    put.add(FAMILY, null, row);
    ht.put(put);
    ht.flushCommitsAsync().get();

    RowLock lock = ht.lockRowAsync(row).get();
    assertTrue(Bytes.equals(lock.getRow(), row));

    ht.unlockRowAsync(lock).get();
  }
}
