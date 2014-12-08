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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test client behavior w/o setting up a cluster.
 * Mock up cluster emissions.
 */
@Category(SmallTests.class)
public class TestClientNoCluster extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TestClientNoCluster.class);
  private Configuration conf;
  public static final ServerName META_SERVERNAME =
      ServerName.valueOf("meta.example.org", 60010, 12345);

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
    // Run my HConnection overrides.  Use my little HConnectionImplementation below which
    // allows me insert mocks and also use my Registry below rather than the default zk based
    // one so tests run faster and don't have zk dependency.
    this.conf.set("hbase.client.registry.impl", SimpleRegistry.class.getName());
  }

  /**
   * Simple cluster registry inserted in place of our usual zookeeper based one.
   */
  static class SimpleRegistry implements Registry {
    final ServerName META_HOST = META_SERVERNAME;

    @Override
    public void init(HConnection connection) {
    }

    @Override
    public HRegionLocation getMetaRegionLocation() throws IOException {
      return new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, META_HOST);
    }

    @Override
    public String getClusterId() {
      return HConstants.CLUSTER_ID_DEFAULT;
    }

    @Override
    public boolean isTableOnlineState(TableName tableName, boolean enabled)
    throws IOException {
      return enabled;
    }

    @Override
    public int getCurrentNrHRS() throws IOException {
      return 1;
    }
  }

  /**
   * Remove the @Ignore to try out timeout and retry asettings
   * @throws IOException
   */
  @Ignore 
  @Test
  public void testTimeoutAndRetries() throws IOException {
    Configuration localConfig = HBaseConfiguration.create(this.conf);
    // This override mocks up our exists/get call to throw a RegionServerStoppedException.
    localConfig.set("hbase.client.connection.impl", RpcTimeoutConnection.class.getName());
    HTable table = new HTable(localConfig, TableName.META_TABLE_NAME);
    Throwable t = null;
    LOG.info("Start");
    try {
      // An exists call turns into a get w/ a flag.
      table.exists(new Get(Bytes.toBytes("abc")));
    } catch (SocketTimeoutException e) {
      // I expect this exception.
      LOG.info("Got expected exception", e);
      t = e;
    } catch (RetriesExhaustedException e) {
      // This is the old, unwanted behavior.  If we get here FAIL!!!
      fail();
    } finally {
      table.close();
    }
    LOG.info("Stop");
    assertTrue(t != null);
  }

  /**
   * Test that operation timeout prevails over rpc default timeout and retries, etc.
   * @throws IOException
   */
  @Test
  public void testRocTimeout() throws IOException {
    Configuration localConfig = HBaseConfiguration.create(this.conf);
    // This override mocks up our exists/get call to throw a RegionServerStoppedException.
    localConfig.set("hbase.client.connection.impl", RpcTimeoutConnection.class.getName());
    int pause = 10;
    localConfig.setInt("hbase.client.pause", pause);
    localConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    // Set the operation timeout to be < the pause.  Expectation is that after first pause, we will
    // fail out of the rpc because the rpc timeout will have been set to the operation tiemout
    // and it has expired.  Otherwise, if this functionality is broke, all retries will be run --
    // all ten of them -- and we'll get the RetriesExhaustedException exception.
    localConfig.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, pause - 1);
    HTable table = new HTable(localConfig, TableName.META_TABLE_NAME);
    Throwable t = null;
    try {
      // An exists call turns into a get w/ a flag.
      table.exists(new Get(Bytes.toBytes("abc")));
    } catch (SocketTimeoutException e) {
      // I expect this exception.
      LOG.info("Got expected exception", e);
      t = e;
    } catch (RetriesExhaustedException e) {
      // This is the old, unwanted behavior.  If we get here FAIL!!!
      fail();
    } finally {
      table.close();
    }
    assertTrue(t != null);
  }

  @Test
  public void testDoNotRetryMetaScanner() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    MetaScanner.metaScan(this.conf, null);
  }

  @Test
  public void testDoNotRetryOnScanNext() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    // Go against meta else we will try to find first region for the table on construction which
    // means we'll have to do a bunch more mocking.  Tests that go against meta only should be
    // good for a bit of testing.
    HTable table = new HTable(this.conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = table.getScanner(HConstants.CATALOG_FAMILY);
    try {
      Result result = null;
      while ((result = scanner.next()) != null) {
        LOG.info(result);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }

  @Test
  public void testRegionServerStoppedOnScannerOpen() throws IOException {
    this.conf.set("hbase.client.connection.impl",
      RegionServerStoppedOnScannerOpenConnection.class.getName());
    // Go against meta else we will try to find first region for the table on construction which
    // means we'll have to do a bunch more mocking.  Tests that go against meta only should be
    // good for a bit of testing.
    HTable table = new HTable(this.conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = table.getScanner(HConstants.CATALOG_FAMILY);
    try {
      Result result = null;
      while ((result = scanner.next()) != null) {
        LOG.info(result);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }

  /**
   * Override to shutdown going to zookeeper for cluster id and meta location.
   */
  static class ScanOpenNextThenExceptionThenRecoverConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    ScanOpenNextThenExceptionThenRecoverConnection(Configuration conf,
        boolean managed, ExecutorService pool) throws IOException {
      super(conf, managed);
      // Mock up my stub so open scanner returns a scanner id and then on next, we throw
      // exceptions for three times and then after that, we return no more to scan.
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      long sid = 12345L;
      try {
        Mockito.when(stub.scan((RpcController)Mockito.any(),
            (ClientProtos.ScanRequest)Mockito.any())).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).build()).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito"))).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).
              setMoreResults(false).build());
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }

  /**
   * Override to shutdown going to zookeeper for cluster id and meta location.
   */
  static class RegionServerStoppedOnScannerOpenConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    RegionServerStoppedOnScannerOpenConnection(Configuration conf, boolean managed,
        ExecutorService pool, User user) throws IOException {
      super(conf, managed);
      // Mock up my stub so open scanner returns a scanner id and then on next, we throw
      // exceptions for three times and then after that, we return no more to scan.
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      long sid = 12345L;
      try {
        Mockito.when(stub.scan((RpcController)Mockito.any(),
            (ClientProtos.ScanRequest)Mockito.any())).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).build()).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito"))).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).
              setMoreResults(false).build());
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }

  /**
   * Override to check we are setting rpc timeout right.
   */
  static class RpcTimeoutConnection
  extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    RpcTimeoutConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
    throws IOException {
      super(conf, managed);
      // Mock up my stub so an exists call -- which turns into a get -- throws an exception
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      try {
        Mockito.when(stub.get((RpcController)Mockito.any(),
            (ClientProtos.GetRequest)Mockito.any())).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito")));
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }

  /**
   * Fake many regionservers and many regions on a connection implementation.
   */
  static class ManyServersManyRegionsConnection
  extends HConnectionManager.HConnectionImplementation {
    // All access should be synchronized
    final Map<ServerName, ClientService.BlockingInterface> serversByClient;

    /**
     * Map of faked-up rows of a 'meta table'.
     */
    final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta;
    final AtomicLong sequenceids = new AtomicLong(0);
    private final Configuration conf;

    ManyServersManyRegionsConnection(Configuration conf, boolean managed,
        ExecutorService pool, User user)
    throws IOException {
      super(conf, managed, pool, user);
      int serverCount = conf.getInt("hbase.test.servers", 10);
      this.serversByClient =
        new HashMap<ServerName, ClientService.BlockingInterface>(serverCount);
      this.meta = makeMeta(Bytes.toBytes(
        conf.get("hbase.test.tablename", Bytes.toString(BIG_USER_TABLE))),
        conf.getInt("hbase.test.regions", 100),
        conf.getLong("hbase.test.namespace.span", 1000),
        serverCount);
      this.conf = conf;
    }

    @Override
    public ClientService.BlockingInterface getClient(ServerName sn) throws IOException {
      // if (!sn.toString().startsWith("meta")) LOG.info(sn);
      ClientService.BlockingInterface stub = null;
      synchronized (this.serversByClient) {
        stub = this.serversByClient.get(sn);
        if (stub == null) {
          stub = new FakeServer(this.conf, meta, sequenceids);
          this.serversByClient.put(sn, stub);
        }
      }
      return stub;
    }
  }

  static MultiResponse doMultiResponse(final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta,
      final AtomicLong sequenceids, final MultiRequest request) {
    // Make a response to match the request.  Act like there were no failures.
    ClientProtos.MultiResponse.Builder builder = ClientProtos.MultiResponse.newBuilder();
    // Per Region.
    RegionActionResult.Builder regionActionResultBuilder =
        RegionActionResult.newBuilder();
    ResultOrException.Builder roeBuilder = ResultOrException.newBuilder();
    for (RegionAction regionAction: request.getRegionActionList()) {
      regionActionResultBuilder.clear();
      // Per Action in a Region.
      for (ClientProtos.Action action: regionAction.getActionList()) {
        roeBuilder.clear();
        // Return empty Result and proper index as result.
        roeBuilder.setResult(ClientProtos.Result.getDefaultInstance());
        roeBuilder.setIndex(action.getIndex());
        regionActionResultBuilder.addResultOrException(roeBuilder.build());
      }
      builder.addRegionActionResult(regionActionResultBuilder.build());
    }
    return builder.build();
  }

  /**
   * Fake 'server'.
   * Implements the ClientService responding as though it were a 'server' (presumes a new
   * ClientService.BlockingInterface made per server).
   */
  static class FakeServer implements ClientService.BlockingInterface {
    private AtomicInteger multiInvocationsCount = new AtomicInteger(0);
    private final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta;
    private final AtomicLong sequenceids;
    private final long multiPause;
    private final int tooManyMultiRequests;

    FakeServer(final Configuration c, final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta,
        final AtomicLong sequenceids) {
      this.meta = meta;
      this.sequenceids = sequenceids;

      // Pause to simulate the server taking time applying the edits.  This will drive up the
      // number of threads used over in client.
      this.multiPause = c.getLong("hbase.test.multi.pause.when.done", 0);
      this.tooManyMultiRequests = c.getInt("hbase.test.multi.too.many", 3);
    }

    @Override
    public GetResponse get(RpcController controller, GetRequest request)
    throws ServiceException {
      boolean metaRegion = isMetaRegion(request.getRegion().getValue().toByteArray(),
        request.getRegion().getType());
      if (!metaRegion) {
        return doGetResponse(request);
      }
      return doMetaGetResponse(meta, request);
    }

    private GetResponse doGetResponse(GetRequest request) {
      ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();
      ByteString row = request.getGet().getRow();
      resultBuilder.addCell(getStartCode(row));
      GetResponse.Builder builder = GetResponse.newBuilder();
      builder.setResult(resultBuilder.build());
      return builder.build();
    }

    @Override
    public MutateResponse mutate(RpcController controller,
        MutateRequest request) throws ServiceException {
      throw new NotImplementedException();
    }

    @Override
    public ScanResponse scan(RpcController controller,
        ScanRequest request) throws ServiceException {
      // Presume it is a scan of meta for now. Not all scans provide a region spec expecting
      // the server to keep reference by scannerid.  TODO.
      return doMetaScanResponse(meta, sequenceids, request);
    }

    @Override
    public BulkLoadHFileResponse bulkLoadHFile(
        RpcController controller, BulkLoadHFileRequest request)
        throws ServiceException {
      throw new NotImplementedException();
    }

    @Override
    public CoprocessorServiceResponse execService(
        RpcController controller, CoprocessorServiceRequest request)
        throws ServiceException {
      throw new NotImplementedException();
    }

    @Override
    public MultiResponse multi(RpcController controller, MultiRequest request)
    throws ServiceException {
      int concurrentInvocations = this.multiInvocationsCount.incrementAndGet();
      try {
        if (concurrentInvocations >= tooManyMultiRequests) {
          throw new ServiceException(new RegionTooBusyException("concurrentInvocations=" +
           concurrentInvocations));
        }
        Threads.sleep(multiPause);
        return doMultiResponse(meta, sequenceids, request);
      } finally {
        this.multiInvocationsCount.decrementAndGet();
      }
    }

    @Override
    public CoprocessorServiceResponse execRegionServerService(RpcController controller,
        CoprocessorServiceRequest request) throws ServiceException {
      throw new NotImplementedException();
    }
  }

  static ScanResponse doMetaScanResponse(final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta,
      final AtomicLong sequenceids, final ScanRequest request) {
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    int max = request.getNumberOfRows();
    int count = 0;
    Map<byte [], Pair<HRegionInfo, ServerName>> tail =
      request.hasScan()? meta.tailMap(request.getScan().getStartRow().toByteArray()): meta;
      ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();
    for (Map.Entry<byte [], Pair<HRegionInfo, ServerName>> e: tail.entrySet()) {
      // Can be 0 on open of a scanner -- i.e. rpc to setup scannerid only.
      if (max <= 0) break;
      if (++count > max) break;
      HRegionInfo hri = e.getValue().getFirst();
      ByteString row = ByteStringer.wrap(hri.getRegionName());
      resultBuilder.clear();
      resultBuilder.addCell(getRegionInfo(row, hri));
      resultBuilder.addCell(getServer(row, e.getValue().getSecond()));
      resultBuilder.addCell(getStartCode(row));
      builder.addResults(resultBuilder.build());
      // Set more to false if we are on the last region in table.
      if (hri.getEndKey().length <= 0) builder.setMoreResults(false);
      else builder.setMoreResults(true);
    }
    // If no scannerid, set one.
    builder.setScannerId(request.hasScannerId()?
      request.getScannerId(): sequenceids.incrementAndGet());
    return builder.build();
  }

  static GetResponse doMetaGetResponse(final SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta,
      final GetRequest request) {
    ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();
    ByteString row = request.getGet().getRow();
    Pair<HRegionInfo, ServerName> p = meta.get(row.toByteArray());
    if (p == null) {
      if (request.getGet().getClosestRowBefore()) {
        byte [] bytes = row.toByteArray();
        SortedMap<byte [], Pair<HRegionInfo, ServerName>> head =
          bytes != null? meta.headMap(bytes): meta;
        p = head == null? null: head.get(head.lastKey());
      }
    }
    if (p != null) {
      resultBuilder.addCell(getRegionInfo(row, p.getFirst()));
      resultBuilder.addCell(getServer(row, p.getSecond()));
    }
    resultBuilder.addCell(getStartCode(row));
    GetResponse.Builder builder = GetResponse.newBuilder();
    builder.setResult(resultBuilder.build());
    return builder.build();
  }

  /**
   * @param name region name or encoded region name.
   * @param type
   * @return True if we are dealing with a hbase:meta region.
   */
  static boolean isMetaRegion(final byte [] name, final RegionSpecifierType type) {
    switch (type) {
    case REGION_NAME:
      return Bytes.equals(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), name);
    case ENCODED_REGION_NAME:
      return Bytes.equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(), name);
    default: throw new UnsupportedOperationException();
    }
  }

  private final static ByteString CATALOG_FAMILY_BYTESTRING =
      ByteStringer.wrap(HConstants.CATALOG_FAMILY);
  private final static ByteString REGIONINFO_QUALIFIER_BYTESTRING =
      ByteStringer.wrap(HConstants.REGIONINFO_QUALIFIER);
  private final static ByteString SERVER_QUALIFIER_BYTESTRING =
      ByteStringer.wrap(HConstants.SERVER_QUALIFIER);

  static CellProtos.Cell.Builder getBaseCellBuilder(final ByteString row) {
    CellProtos.Cell.Builder cellBuilder = CellProtos.Cell.newBuilder();
    cellBuilder.setRow(row);
    cellBuilder.setFamily(CATALOG_FAMILY_BYTESTRING);
    cellBuilder.setTimestamp(System.currentTimeMillis());
    return cellBuilder;
  }

  static CellProtos.Cell getRegionInfo(final ByteString row, final HRegionInfo hri) {
    CellProtos.Cell.Builder cellBuilder = getBaseCellBuilder(row);
    cellBuilder.setQualifier(REGIONINFO_QUALIFIER_BYTESTRING);
    cellBuilder.setValue(ByteStringer.wrap(hri.toByteArray()));
    return cellBuilder.build();
  }

  static CellProtos.Cell getServer(final ByteString row, final ServerName sn) {
    CellProtos.Cell.Builder cellBuilder = getBaseCellBuilder(row);
    cellBuilder.setQualifier(SERVER_QUALIFIER_BYTESTRING);
    cellBuilder.setValue(ByteString.copyFromUtf8(sn.getHostAndPort()));
    return cellBuilder.build();
  }

  static CellProtos.Cell getStartCode(final ByteString row) {
    CellProtos.Cell.Builder cellBuilder = getBaseCellBuilder(row);
    cellBuilder.setQualifier(ByteStringer.wrap(HConstants.STARTCODE_QUALIFIER));
    // TODO:
    cellBuilder.setValue(ByteStringer.wrap(Bytes.toBytes(META_SERVERNAME.getStartcode())));
    return cellBuilder.build();
  }

  private static final byte [] BIG_USER_TABLE = Bytes.toBytes("t");

  /**
   * Format passed integer.  Zero-pad.
   * Copied from hbase-server PE class and small amendment.  Make them share.
   * @param number
   * @return Returns zero-prefixed 10-byte wide decimal version of passed
   * number (Does absolute in case number is negative).
   */
  private static byte [] format(final long number) {
    byte [] b = new byte[10];
    long d = number;
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return b;
  }

  /**
   * @param count
   * @param namespaceSpan
   * @return <code>count</code> regions
   */
  private static HRegionInfo [] makeHRegionInfos(final byte [] tableName, final int count,
      final long namespaceSpan) {
    byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
    byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
    long interval = namespaceSpan / count;
    HRegionInfo [] hris = new HRegionInfo[count];
    for (int i = 0; i < count; i++) {
      if (i == 0) {
        endKey = format(interval);
      } else {
        startKey = endKey;
        if (i == count - 1) endKey = HConstants.EMPTY_BYTE_ARRAY;
        else endKey = format((i + 1) * interval);
      }
      hris[i] = new HRegionInfo(TableName.valueOf(tableName), startKey, endKey);
    }
    return hris;
  }

  /**
   * @param count
   * @return Return <code>count</code> servernames.
   */
  private static ServerName [] makeServerNames(final int count) {
    ServerName [] sns = new ServerName[count];
    for (int i = 0; i < count; i++) {
      sns[i] = ServerName.valueOf("" + i + ".example.org", 60010, i);
    }
    return sns;
  }

  /**
   * Comparator for meta row keys.
   */
  private static class MetaRowsComparator implements Comparator<byte []> {
    private final KeyValue.KVComparator delegate = new KeyValue.MetaComparator();
    @Override
    public int compare(byte[] left, byte[] right) {
      return delegate.compareRows(left, 0, left.length, right, 0, right.length);
    }
  }

  /**
   * Create up a map that is keyed by meta row name and whose value is the HRegionInfo and
   * ServerName to return for this row.
   * @return Map with faked hbase:meta content in it.
   */
  static SortedMap<byte [], Pair<HRegionInfo, ServerName>> makeMeta(final byte [] tableName,
      final int regionCount, final long namespaceSpan, final int serverCount) {
    // I need a comparator for meta rows so we sort properly.
    SortedMap<byte [], Pair<HRegionInfo, ServerName>> meta =
      new ConcurrentSkipListMap<byte[], Pair<HRegionInfo,ServerName>>(new MetaRowsComparator());
    HRegionInfo [] hris = makeHRegionInfos(tableName, regionCount, namespaceSpan);
    ServerName [] serverNames = makeServerNames(serverCount);
    int per = regionCount / serverCount;
    int count = 0;
    for (HRegionInfo hri: hris) {
      Pair<HRegionInfo, ServerName> p =
        new Pair<HRegionInfo, ServerName>(hri, serverNames[count++ / per]);
      meta.put(hri.getRegionName(), p);
    }
    return meta;
  }

  /**
   * Code for each 'client' to run.
   *
   * @param id
   * @param c
   * @param sharedConnection
   * @throws IOException
   */
  static void cycle(int id, final Configuration c, final HConnection sharedConnection) throws IOException {
    HTableInterface table = sharedConnection.getTable(BIG_USER_TABLE);
    table.setAutoFlushTo(false);
    long namespaceSpan = c.getLong("hbase.test.namespace.span", 1000000);
    long startTime = System.currentTimeMillis();
    final int printInterval = 100000;
    Random rd = new Random(id);
    boolean get = c.getBoolean("hbase.test.do.gets", false);
    try {
      Stopwatch stopWatch = new Stopwatch();
      stopWatch.start();
      for (int i = 0; i < namespaceSpan; i++) {
        byte [] b = format(rd.nextLong());
        if (get){
          Get g = new Get(b);
          table.get(g);
        } else {
          Put p = new Put(b);
          p.add(HConstants.CATALOG_FAMILY, b, b);
          table.put(p);
        }
        if (i % printInterval == 0) {
          LOG.info("Put " + printInterval + "/" + stopWatch.elapsedMillis());
          stopWatch.reset();
          stopWatch.start();
        }
      }
      LOG.info("Finished a cycle putting " + namespaceSpan + " in " +
          (System.currentTimeMillis() - startTime) + "ms");
    } finally {
      table.close();
    }
  }

  @Override
  public int run(String[] arg0) throws Exception {
    int errCode = 0;
    // TODO: Make command options.
    // How many servers to fake.
    final int servers = 1;
    // How many regions to put on the faked servers.
    final int regions = 100000;
    // How many 'keys' in the faked regions.
    final long namespaceSpan = 50000000;
    // How long to take to pause after doing a put; make this long if you want to fake a struggling
    // server.
    final long multiPause = 0;
    // Check args make basic sense.
    if ((namespaceSpan < regions) || (regions < servers)) {
      throw new IllegalArgumentException("namespaceSpan=" + namespaceSpan + " must be > regions=" +
        regions + " which must be > servers=" + servers);
    }

    // Set my many servers and many regions faking connection in place.
    getConf().set("hbase.client.connection.impl",
      ManyServersManyRegionsConnection.class.getName());
    // Use simple kv registry rather than zk
    getConf().set("hbase.client.registry.impl", SimpleRegistry.class.getName());
    // When to report fails.  Default is we report the 10th.  This means we'll see log everytime
    // an exception is thrown -- usually RegionTooBusyException when we have more than
    // hbase.test.multi.too.many requests outstanding at any time.
    getConf().setInt("hbase.client.start.log.errors.counter", 0);
 
    // Ugly but this is only way to pass in configs.into ManyServersManyRegionsConnection class.
    getConf().setInt("hbase.test.regions", regions);
    getConf().setLong("hbase.test.namespace.span", namespaceSpan);
    getConf().setLong("hbase.test.servers", servers);
    getConf().set("hbase.test.tablename", Bytes.toString(BIG_USER_TABLE));
    getConf().setLong("hbase.test.multi.pause.when.done", multiPause);
    // Let there be ten outstanding requests at a time before we throw RegionBusyException.
    getConf().setInt("hbase.test.multi.too.many", 10);
    final int clients = 2;

    // Have them all share the same connection so they all share the same instance of
    // ManyServersManyRegionsConnection so I can keep an eye on how many requests by server.
    final ExecutorService pool = Executors.newCachedThreadPool(Threads.getNamedThreadFactory("p"));
      // Executors.newFixedThreadPool(servers * 10, Threads.getNamedThreadFactory("p"));
    // Share a connection so I can keep counts in the 'server' on concurrency.
    final HConnection sharedConnection = HConnectionManager.createConnection(getConf()/*, pool*/);
    try {
      Thread [] ts = new Thread[clients];
      for (int j = 0; j < ts.length; j++) {
        final int id = j;
        ts[j] = new Thread("" + j) {
          final Configuration c = getConf();

          @Override
          public void run() {
            try {
              cycle(id, c, sharedConnection);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        };
        ts[j].start();
      }
      for (int j = 0; j < ts.length; j++) {
        ts[j].join();
      }
    } finally {
      sharedConnection.close();
    }
    return errCode;
  }

  /**
   * Run a client instance against a faked up server.
   * @param args TODO
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new TestClientNoCluster(), args));
  }
}