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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos.TestArg;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestDelayedRpcProtos.TestResponse;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;

/**
 * Test that delayed RPCs work. Fire up three calls, the first of which should
 * be delayed. Check that the last two, which are undelayed, return before the
 * first one.
 */
@Category(MediumTests.class) // Fails sometimes with small tests
public class TestDelayedRpc {
  public static RpcServer rpcServer;

  public static final int UNDELAYED = 0;
  public static final int DELAYED = 1;

  @Test
  public void testDelayedRpcImmediateReturnValue() throws Exception {
    testDelayedRpc(false);
  }

  @Test
  public void testDelayedRpcDelayedReturnValue() throws Exception {
    testDelayedRpc(true);
  }

  private void testDelayedRpc(boolean delayReturnValue) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    TestRpcImpl instance = new TestRpcImpl(delayReturnValue);
    rpcServer = HBaseServerRPC.getServer(instance.getClass(), instance,
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();

    ProtobufRpcClientEngine clientEngine = new ProtobufRpcClientEngine(conf);
    try {
      TestRpc client = clientEngine.getProxy(TestRpc.class,
          rpcServer.getListenerAddress(), conf, 1000);

      List<Integer> results = new ArrayList<Integer>();

      TestThread th1 = new TestThread(client, true, results);
      TestThread th2 = new TestThread(client, false, results);
      TestThread th3 = new TestThread(client, false, results);
      th1.start();
      Thread.sleep(100);
      th2.start();
      Thread.sleep(200);
      th3.start();

      th1.join();
      th2.join();
      th3.join();

      assertEquals(UNDELAYED, results.get(0).intValue());
      assertEquals(UNDELAYED, results.get(1).intValue());
      assertEquals(results.get(2).intValue(), delayReturnValue ? DELAYED :
          0xDEADBEEF);
    } finally {
      clientEngine.close();
    }
  }

  private static class ListAppender extends AppenderSkeleton {
    private List<String> messages = new ArrayList<String>();

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

  @Test
  public void testTooManyDelayedRpcs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final int MAX_DELAYED_RPC = 10;
    conf.setInt("hbase.ipc.warn.delayedrpc.number", MAX_DELAYED_RPC);

    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer");
    log.addAppender(listAppender);
    log.setLevel(Level.WARN);

    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    TestRpcImpl instance = new TestRpcImpl(true);
    rpcServer = HBaseServerRPC.getServer(instance.getClass(), instance,
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();

    ProtobufRpcClientEngine clientEngine = new ProtobufRpcClientEngine(conf);
    try {
      TestRpc client = clientEngine.getProxy(TestRpc.class,
          rpcServer.getListenerAddress(), conf, 1000);

      Thread threads[] = new Thread[MAX_DELAYED_RPC + 1];

      for (int i = 0; i < MAX_DELAYED_RPC; i++) {
        threads[i] = new TestThread(client, true, null);
        threads[i].start();
      }

      /* No warnings till here. */
      assertTrue(listAppender.getMessages().isEmpty());

      /* This should give a warning. */
      threads[MAX_DELAYED_RPC] = new TestThread(client, true, null);
      threads[MAX_DELAYED_RPC].start();

      for (int i = 0; i < MAX_DELAYED_RPC; i++) {
        threads[i].join();
      }

      assertFalse(listAppender.getMessages().isEmpty());
      assertTrue(listAppender.getMessages().get(0).startsWith(
          "Too many delayed calls"));

      log.removeAppender(listAppender);
    } finally {
      clientEngine.close();
    }
  }

  public interface TestRpc extends IpcProtocol {
    TestResponse test(TestArg delay);
  }

  private static class TestRpcImpl implements TestRpc {
    /**
     * Should the return value of delayed call be set at the end of the delay
     * or at call return.
     */
    private boolean delayReturnValue;

    /**
     * @param delayReturnValue Should the response to the delayed call be set
     * at the start or the end of the delay.
     */
    public TestRpcImpl(boolean delayReturnValue) {
      this.delayReturnValue = delayReturnValue;
    }

    @Override
    public TestResponse test(final TestArg testArg) {
      boolean delay = testArg.getDelay();
      TestResponse.Builder responseBuilder = TestResponse.newBuilder();
      if (!delay) {
        responseBuilder.setResponse(UNDELAYED);
        return responseBuilder.build();
      }
      final Delayable call = HBaseServer.getCurrentCall();
      call.startDelay(delayReturnValue);
      new Thread() {
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

  private static class TestThread extends Thread {
    private TestRpc server;
    private boolean delay;
    private List<Integer> results;

    public TestThread(TestRpc server, boolean delay, List<Integer> results) {
      this.server = server;
      this.delay = delay;
      this.results = results;
    }

    @Override
    public void run() {
      try {
        Integer result = 
            new Integer(server.test(TestArg.newBuilder()
                .setDelay(delay).build()).getResponse());
        if (results != null) {
          synchronized (results) {
            results.add(result);
          }
        }
      } catch (Exception e) {
         fail("Unexpected exception: "+e.getMessage());
      }
    }
  }

  @Test
  public void testEndDelayThrowing() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    FaultyTestRpc instance = new FaultyTestRpc();
    rpcServer = HBaseServerRPC.getServer(instance.getClass(), instance,
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();

    ProtobufRpcClientEngine clientEngine = new ProtobufRpcClientEngine(conf);
    try {
      TestRpc client = clientEngine.getProxy(TestRpc.class,
          rpcServer.getListenerAddress(), conf, 1000);

      int result = 0xDEADBEEF;

      try {
        result = client.test(TestArg.newBuilder().setDelay(false).build()).getResponse();
      } catch (Exception e) {
        fail("No exception should have been thrown.");
      }
      assertEquals(result, UNDELAYED);

      boolean caughtException = false;
      try {
        result = client.test(TestArg.newBuilder().setDelay(true).build()).getResponse();
      } catch(Exception e) {
        // Exception thrown by server is enclosed in a RemoteException.
        if (e.getCause().getMessage().contains(
            "java.lang.Exception: Something went wrong"))
          caughtException = true;
        Log.warn(e);
      }
      assertTrue(caughtException);
    } finally {
      clientEngine.close();
    }
  }

  /**
   * Delayed calls to this class throw an exception.
   */
  private static class FaultyTestRpc implements TestRpc {
    @Override
    public TestResponse test(TestArg arg) {
      if (!arg.getDelay())
        return TestResponse.newBuilder().setResponse(UNDELAYED).build();
      Delayable call = HBaseServer.getCurrentCall();
      call.startDelay(true);
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