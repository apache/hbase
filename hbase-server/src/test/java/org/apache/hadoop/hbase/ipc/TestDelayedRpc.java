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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos.TestArg;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos.TestResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test that delayed RPCs work. Fire up three calls, the first of which should
 * be delayed. Check that the last two, which are undelayed, return before the
 * first one.
 */
@Category(MediumTests.class) // Fails sometimes with small tests
public class TestDelayedRpc {
  private static final Log LOG = LogFactory.getLog(TestDelayedRpc.class);
  public static RpcServerInterface rpcServer;
  public static final int UNDELAYED = 0;
  public static final int DELAYED = 1;
  private static final int RPC_CLIENT_TIMEOUT = 30000;

  @Test (timeout=60000)
  public void testDelayedRpcImmediateReturnValue() throws Exception {
    testDelayedRpc(false);
  }

  @Test (timeout=60000)
  public void testDelayedRpcDelayedReturnValue() throws Exception {
    testDelayedRpc(true);
  }

  private void testDelayedRpc(boolean delayReturnValue) throws Exception {
    LOG.info("Running testDelayedRpc delayReturnValue=" + delayReturnValue);
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    TestDelayedImplementation instance = new TestDelayedImplementation(delayReturnValue);
    BlockingService service =
      TestDelayedRpcProtos.TestDelayedService.newReflectiveBlockingService(instance);
    rpcServer = new RpcServer(null, "testDelayedRpc",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
        isa,
        conf,
        new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient = new RpcClient(conf, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(rpcServer.getListenerAddress().getHostName(),
              rpcServer.getListenerAddress().getPort(), System.currentTimeMillis()),
          User.getCurrent(), RPC_CLIENT_TIMEOUT);
      TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub =
        TestDelayedRpcProtos.TestDelayedService.newBlockingStub(channel);
      List<Integer> results = new ArrayList<Integer>();
      // Setting true sets 'delayed' on the client.
      TestThread th1 = new TestThread(stub, true, results);
      // Setting 'false' means we will return UNDELAYED as response immediately.
      TestThread th2 = new TestThread(stub, false, results);
      TestThread th3 = new TestThread(stub, false, results);
      th1.start();
      Thread.sleep(100);
      th2.start();
      Thread.sleep(200);
      th3.start();

      th1.join();
      th2.join();
      th3.join();

      // We should get the two undelayed responses first.
      assertEquals(UNDELAYED, results.get(0).intValue());
      assertEquals(UNDELAYED, results.get(1).intValue());
      assertEquals(results.get(2).intValue(), delayReturnValue ? DELAYED :  0xDEADBEEF);
    } finally {
      rpcClient.stop();
    }
  }

  private static class ListAppender extends AppenderSkeleton {
    private final List<String> messages = new ArrayList<String>();

    @Override
    protected void append(LoggingEvent event) {
      messages.add(event.getMessage().toString());
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    public List<String> getMessages() {
      return messages;
    }
  }

  /**
   * Tests that we see a WARN message in the logs.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testTooManyDelayedRpcs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final int MAX_DELAYED_RPC = 10;
    conf.setInt("hbase.ipc.warn.delayedrpc.number", MAX_DELAYED_RPC);
    // Set up an appender to catch the "Too many delayed calls" that we expect.
    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.RpcServer");
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);


    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    TestDelayedImplementation instance = new TestDelayedImplementation(true);
    BlockingService service =
      TestDelayedRpcProtos.TestDelayedService.newReflectiveBlockingService(instance);
    rpcServer = new RpcServer(null, "testTooManyDelayedRpcs",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
        isa,
        conf,
        new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient = new RpcClient(conf, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(rpcServer.getListenerAddress().getHostName(),
              rpcServer.getListenerAddress().getPort(), System.currentTimeMillis()),
          User.getCurrent(), RPC_CLIENT_TIMEOUT);
      TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub =
        TestDelayedRpcProtos.TestDelayedService.newBlockingStub(channel);
      Thread threads[] = new Thread[MAX_DELAYED_RPC + 1];
      for (int i = 0; i < MAX_DELAYED_RPC; i++) {
        threads[i] = new TestThread(stub, true, null);
        threads[i].start();
      }

      /* No warnings till here. */
      assertTrue(listAppender.getMessages().isEmpty());

      /* This should give a warning. */
      threads[MAX_DELAYED_RPC] = new TestThread(stub, true, null);
      threads[MAX_DELAYED_RPC].start();

      for (int i = 0; i < MAX_DELAYED_RPC; i++) {
        threads[i].join();
      }

      assertFalse(listAppender.getMessages().isEmpty());
      assertTrue(listAppender.getMessages().get(0).startsWith("Too many delayed calls"));

      log.removeAppender(listAppender);
    } finally {
      rpcClient.stop();
    }
  }

  public static class TestDelayedImplementation
  implements TestDelayedRpcProtos.TestDelayedService.BlockingInterface {
    /**
     * Should the return value of delayed call be set at the end of the delay
     * or at call return.
     */
    private final boolean delayReturnValue;

    /**
     * @param delayReturnValue Should the response to the delayed call be set
     * at the start or the end of the delay.
     */
    public TestDelayedImplementation(boolean delayReturnValue) {
      this.delayReturnValue = delayReturnValue;
    }

    @Override
    public TestResponse test(final RpcController rpcController, final TestArg testArg)
    throws ServiceException {
      boolean delay = testArg.getDelay();
      TestResponse.Builder responseBuilder = TestResponse.newBuilder();
      if (!delay) {
        responseBuilder.setResponse(UNDELAYED);
        return responseBuilder.build();
      }
      final Delayable call = RpcServer.getCurrentCall();
      call.startDelay(delayReturnValue);
      new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(500);
            TestResponse.Builder responseBuilder = TestResponse.newBuilder();
            call.endDelay(delayReturnValue ?
                responseBuilder.setResponse(DELAYED).build() : null);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }.start();
      // This value should go back to client only if the response is set
      // immediately at delay time.
      responseBuilder.setResponse(0xDEADBEEF);
      return responseBuilder.build();
    }
  }

  public static class TestThread extends Thread {
    private final TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub;
    private final boolean delay;
    private final List<Integer> results;

    public TestThread(TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub,
        boolean delay, List<Integer> results) {
      this.stub = stub;
      this.delay = delay;
      this.results = results;
    }

    @Override
    public void run() {
      Integer result;
      try {
        result = new Integer(stub.test(null, TestArg.newBuilder().setDelay(delay).build()).
          getResponse());
      } catch (ServiceException e) {
        throw new RuntimeException(e);
      }
      if (results != null) {
        synchronized (results) {
          results.add(result);
        }
      }
    }
  }

  @Test
  public void testEndDelayThrowing() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    FaultyTestDelayedImplementation instance = new FaultyTestDelayedImplementation();
    BlockingService service =
      TestDelayedRpcProtos.TestDelayedService.newReflectiveBlockingService(instance);
    rpcServer = new RpcServer(null, "testEndDelayThrowing",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(service, null)),
        isa,
        conf,
        new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient = new RpcClient(conf, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
          ServerName.valueOf(rpcServer.getListenerAddress().getHostName(),
              rpcServer.getListenerAddress().getPort(), System.currentTimeMillis()),
        User.getCurrent(), 1000);
      TestDelayedRpcProtos.TestDelayedService.BlockingInterface stub =
        TestDelayedRpcProtos.TestDelayedService.newBlockingStub(channel);

      int result = 0xDEADBEEF;

      try {
        result = stub.test(null, TestArg.newBuilder().setDelay(false).build()).getResponse();
      } catch (Exception e) {
        fail("No exception should have been thrown.");
      }
      assertEquals(result, UNDELAYED);

      boolean caughtException = false;
      try {
        result = stub.test(null, TestArg.newBuilder().setDelay(true).build()).getResponse();
      } catch(Exception e) {
        // Exception thrown by server is enclosed in a RemoteException.
        if (e.getCause().getMessage().contains("java.lang.Exception: Something went wrong")) {
          caughtException = true;
        }
        LOG.warn("Caught exception, expected=" + caughtException);
      }
      assertTrue(caughtException);
    } finally {
      rpcClient.stop();
    }
  }

  /**
   * Delayed calls to this class throw an exception.
   */
  private static class FaultyTestDelayedImplementation extends TestDelayedImplementation {
    public FaultyTestDelayedImplementation() {
      super(false);
    }

    @Override
    public TestResponse test(RpcController rpcController, TestArg arg)
    throws ServiceException {
      LOG.info("In faulty test, delay=" + arg.getDelay());
      if (!arg.getDelay()) return TestResponse.newBuilder().setResponse(UNDELAYED).build();
      Delayable call = RpcServer.getCurrentCall();
      call.startDelay(true);
      LOG.info("In faulty test, delaying");
      try {
        call.endDelayThrowing(new Exception("Something went wrong"));
      } catch (IOException e) {
        e.printStackTrace();
      }
      // Client will receive the Exception, not this value.
      return TestResponse.newBuilder().setResponse(DELAYED).build();
    }
  }
}
