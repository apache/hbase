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
package org.apache.hadoop.hbase.ipc;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Category({SmallTests.class})
public class TestRpcHandlerException {
  public static final Log LOG = LogFactory.getLog(TestRpcHandlerException.class);
  static String example = "xyz";
  static byte[] CELL_BYTES = example.getBytes();
  static Cell CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

  private final static Configuration CONF = HBaseConfiguration.create();
  RpcExecutor rpcExecutor = Mockito.mock(RpcExecutor.class);

  // We are using the test TestRpcServiceProtos generated classes and Service because they are
  // available and basic with methods like 'echo', and ping. Below we make a blocking service
  // by passing in implementation of blocking interface. We use this service in all tests that
  // follow.
  private static final BlockingService SERVICE =
      TestRpcServiceProtos.TestProtobufRpcProto
      .newReflectiveBlockingService(new TestRpcServiceProtos
		  .TestProtobufRpcProto.BlockingInterface() {

        @Override
        public EmptyResponseProto ping(RpcController controller, EmptyRequestProto request)
            throws ServiceException {
          return null;
        }

        @Override
        public EmptyResponseProto error(RpcController controller, EmptyRequestProto request)
            throws ServiceException {
          return null;
        }

        @Override
        public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
            throws Error, RuntimeException {
          if (controller instanceof PayloadCarryingRpcController) {
            PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) controller;
            // If cells, scan them to check we are able to iterate what we were given and since
            // this is
            // an echo, just put them back on the controller creating a new block. Tests our
            // block
            // building.
            CellScanner cellScanner = pcrc.cellScanner();
            List<Cell> list = null;
            if (cellScanner != null) {
		list = new ArrayList<Cell>();
		try {
			while (cellScanner.advance()) {
				list.add(cellScanner.current());
				throw new StackOverflowError();
			}
		} catch (StackOverflowError e) {
			throw e;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
            }
            cellScanner = CellUtil.createCellScanner(list);
            ((PayloadCarryingRpcController) controller).setCellScanner(cellScanner);
          }
          return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
        }
      });

  /**
   * Instance of server. We actually don't do anything speical in here so could just use
   * HBaseRpcServer directly.
   */
  private static class TestRpcServer extends RpcServer {

    TestRpcServer() throws IOException {
      this(new FifoRpcScheduler(CONF, 1));
    }

    TestRpcServer(RpcScheduler scheduler) throws IOException {
      super(null, "testRpcServer",
		  Lists.newArrayList(new BlockingServiceAndInterface(SERVICE, null)),
		  new InetSocketAddress("localhost", 0), CONF, scheduler);
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
          throws IOException {
      return super.call(service, md, param, cellScanner, receiveTime, status);
    }
  }

  /** Tests that the rpc scheduler is called when requests arrive.
   *  When Rpc handler thread dies, the client will hang and the test will fail.
   *  The test is meant to be a unit test to test the behavior.
   *
   * */
  private class AbortServer implements Abortable {
    private boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }
  }

  /* This is a unit test to make sure to abort region server when the number of Rpc handler thread
   * caught errors exceeds the threshold. Client will hang when RS aborts.
   */
  @Ignore
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    Abortable abortable = new AbortServer();
    RpcScheduler scheduler = new SimpleRpcScheduler(CONF, 2, 0, 0, qosFunction, abortable, 0);
    RpcServer rpcServer = new TestRpcServer(scheduler);
    RpcClientImpl client = new RpcClientImpl(CONF, HConstants.CLUSTER_ID_DEFAULT);
    try {
      rpcServer.start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      client.call(null, md, param, CellUtil.createCellScanner(ImmutableList.of(CELL)), md
        .getOutputType().toProto(), User.getCurrent(), address, 0);
    } catch (Throwable e) {
      assert(abortable.isAborted() == true);
    } finally {
      rpcServer.stop();
    }
  }

}
