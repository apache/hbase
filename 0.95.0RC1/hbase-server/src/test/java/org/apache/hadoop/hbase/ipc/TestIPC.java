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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.Message;

@Category(SmallTests.class)
public class TestIPC {
  public static final Log LOG = LogFactory.getLog(TestIPC.class);
  static byte [] CELL_BYTES =  Bytes.toBytes("xyz");
  static Cell CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

  private static class TestRpcServer extends HBaseServer {
    TestRpcServer() throws IOException {
      super("0.0.0.0", 0, 1, 1, HBaseConfiguration.create(), "TestRpcServer", 0);
    }

    @Override
    public Pair<Message, CellScanner> call(Class<? extends IpcProtocol> protocol, Method method,
        Message param, final CellScanner cells, long receiveTime, MonitoredRPCHandler status)
    throws IOException {
      /*
      List<Cell> cellsOut = new ArrayList<Cell>();
      while (cells.advance()) {
        Cell cell = cells.current();
        Bytes.equals(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          CELL_BYTES, 0, CELL_BYTES.length);
        cellsOut.add(cell);
      }
      return new Pair<Message, CellScanner>(param, CellUtil.createCellScanner(cellsOut));
      */
      return new Pair<Message, CellScanner>(param, null);
    }
  }

  /**
   * A nothing protocol used in test below.
   */
  interface NothingProtocol extends IpcProtocol {
    void doNothing();
  }

  public static class DoNothing implements NothingProtocol {
    public void doNothing() {}
  }

  @Test
  public void testCompressCellBlock()
  throws IOException, InterruptedException, SecurityException, NoSuchMethodException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    TestRpcServer rpcServer = new TestRpcServer();
    HBaseClient client = new HBaseClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    List<Cell> cells = new ArrayList<Cell>();
    cells.add(CELL);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      // Get any method name... just so it is not null
      Method m = NothingProtocol.class.getMethod("doNothing");
      client.call(m, null, CellUtil.createCellScanner(cells), address, NothingProtocol.class,
        User.getCurrent(), 0);
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }

  @Test
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    SocketFactory spyFactory = spy(NetUtils.getDefaultSocketFactory(conf));
    Mockito.doAnswer(new Answer<Socket>() {
      @Override
      public Socket answer(InvocationOnMock invocation) throws Throwable {
        Socket s = spy((Socket)invocation.callRealMethod());
        doThrow(new RuntimeException("Injected fault")).when(s).setSoTimeout(anyInt());
        return s;
      }
    }).when(spyFactory).createSocket();

    TestRpcServer rpcServer = new TestRpcServer();
    HBaseClient client = new HBaseClient(conf, HConstants.CLUSTER_ID_DEFAULT, spyFactory);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      client.call(null, null, null, address, null, User.getCurrent(), 0);
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }

  public static void main(String[] args)
  throws IOException, SecurityException, NoSuchMethodException, InterruptedException {
    if (args.length != 2) {
      System.out.println("Usage: TestIPC <CYCLES> <CELLS_PER_CYCLE>");
      return;
    }
    // ((Log4JLogger)HBaseServer.LOG).getLogger().setLevel(Level.INFO);
    // ((Log4JLogger)HBaseClient.LOG).getLogger().setLevel(Level.INFO);
    int cycles = Integer.parseInt(args[0]);
    int cellcount = Integer.parseInt(args[1]);
    Configuration conf = HBaseConfiguration.create();
    TestRpcServer rpcServer = new TestRpcServer();
    HBaseClient client = new HBaseClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    KeyValue kv = KeyValueUtil.ensureKeyValue(CELL);
    Put p = new Put(kv.getRow());
    for (int i = 0; i < cellcount; i++) {
      p.add(kv);
    }
    RowMutations rm = new RowMutations(kv.getRow());
    rm.add(p);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      // Get any method name... just so it is not null
      Method m = NothingProtocol.class.getMethod("doNothing");
      long startTime = System.currentTimeMillis();
      User user = User.getCurrent();
      for (int i = 0; i < cycles; i++) {
        List<CellScannable> cells = new ArrayList<CellScannable>();
        // Message param = RequestConverter.buildMultiRequest(HConstants.EMPTY_BYTE_ARRAY, rm);
        Message param = RequestConverter.buildNoDataMultiRequest(HConstants.EMPTY_BYTE_ARRAY, rm, cells);
        CellScanner cellScanner = CellUtil.createCellScanner(cells);
        if (i % 1000 == 0) {
          LOG.info("" + i);
          // Uncomment this for a thread dump every so often.
          // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
          //  "Thread dump " + Thread.currentThread().getName());
        }
        Pair<Message, CellScanner> response =
          client.call(m, param, cellScanner, address, NothingProtocol.class, user, 0);
        /*
        int count = 0;
        while (p.getSecond().advance()) {
          count++;
        }
        assertEquals(cells.size(), count);*/
      }
      LOG.info("Cycled " + cycles + " time(s) with " + cellcount + " cell(s) in " +
         (System.currentTimeMillis() - startTime) + "ms");
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }
}
