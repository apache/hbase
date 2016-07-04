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

import static org.apache.hadoop.hbase.ipc.RpcClient.SPECIFIC_WRITE_THREAD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

@Category(IntegrationTests.class)
public class IntegrationTestRpcClient {

  private static final Log LOG = LogFactory.getLog(IntegrationTestRpcClient.class);

  private final Configuration conf;

  private int numIterations = 10;

  public IntegrationTestRpcClient() {
    conf = HBaseConfiguration.create();
  }

  static class TestRpcServer extends RpcServer {

    TestRpcServer(Configuration conf) throws IOException {
      this(new FifoRpcScheduler(conf, 1), conf);
    }

    TestRpcServer(RpcScheduler scheduler, Configuration conf) throws IOException {
      super(null, "testRpcServer", Lists
          .newArrayList(new BlockingServiceAndInterface(SERVICE, null)), new InetSocketAddress(
          "localhost", 0), conf, scheduler);
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
        Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
        throws IOException {
      return super.call(service, md, param, cellScanner, receiveTime, status);
    }
  }

  static final BlockingService SERVICE =
      TestRpcServiceProtos.TestProtobufRpcProto
      .newReflectiveBlockingService(new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

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
            throws ServiceException {
          return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
        }
      });

  protected AbstractRpcClient createRpcClient(Configuration conf, boolean isSyncClient) {
    return isSyncClient ?
        new RpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT) :
          new AsyncRpcClient(conf) {
          @Override
          Codec getCodec() {
            return null;
          }
        };
  }

  static String BIG_PAYLOAD;

  static {
    StringBuilder builder = new StringBuilder();

    while (builder.length() < 1024 * 1024) { // 2 MB
      builder.append("big.payload.");
    }

    BIG_PAYLOAD = builder.toString();
  }

  class Cluster {
    Random random = new Random();
    ReadWriteLock lock = new ReentrantReadWriteLock();
    HashMap<InetSocketAddress, TestRpcServer> rpcServers = new HashMap<>();
    List<TestRpcServer> serverList = new ArrayList<>();
    int maxServers;
    int minServers;

    Cluster(int minServers, int maxServers) {
      this.minServers = minServers;
      this.maxServers = maxServers;
    }

    TestRpcServer startServer() throws IOException {
      lock.writeLock().lock();
      try {
        if (rpcServers.size() >= maxServers) {
          return null;
        }

        TestRpcServer rpcServer = new TestRpcServer(conf);
        rpcServer.start();
        InetSocketAddress address = rpcServer.getListenerAddress();        
        if (address == null) {
          throw new IOException("Listener channel is closed");
        }
        rpcServers.put(address, rpcServer);
        serverList.add(rpcServer);
        LOG.info("Started server: " + address);
        return rpcServer;
      } finally {
        lock.writeLock().unlock();
      }
    }

    void stopRandomServer() throws Exception {
      lock.writeLock().lock();
      TestRpcServer rpcServer = null;
      try {
        if (rpcServers.size() <= minServers) {
          return;
        }
        int size = rpcServers.size();
        int rand = random.nextInt(size);
        rpcServer = serverList.remove(rand);
        InetSocketAddress address = rpcServer.getListenerAddress();
        if (address == null) {
          // Throw exception here. We can't remove this instance from the server map because
          // we no longer have access to its map key
          throw new IOException("Listener channel is closed");
        }
        rpcServers.remove(address);

        if (rpcServer != null) {
          stopServer(rpcServer);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }

    void stopServer(TestRpcServer rpcServer) throws InterruptedException {
      InetSocketAddress address = rpcServer.getListenerAddress();
      LOG.info("Stopping server: " + address);
      rpcServer.stop();
      rpcServer.join();
      LOG.info("Stopped server: " + address);
    }

    void stopRunning() throws InterruptedException {
      lock.writeLock().lock();
      try {
        for (TestRpcServer rpcServer : serverList) {
          stopServer(rpcServer);
        }

      } finally {
        lock.writeLock().unlock();
      }
    }

    TestRpcServer getRandomServer() {
      lock.readLock().lock();
      try {
        int size = rpcServers.size();
        int rand = random.nextInt(size);
        return serverList.get(rand);
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  static class MiniChaosMonkey extends Thread {
    AtomicBoolean running = new  AtomicBoolean(true);
    Random random = new Random();
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    Cluster cluster;

    public MiniChaosMonkey(Cluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public void run() {
      while (running.get()) {
        switch (random.nextInt() % 2) {
        case 0: //start a server
          try {
            cluster.startServer();
          } catch (Exception e) {
            LOG.warn(e);
            exception.compareAndSet(null, e);
          }
          break;

        case 1: // stop a server
          try {
            cluster.stopRandomServer();
          } catch (Exception e) {
            LOG.warn(e);
            exception.compareAndSet(null, e);
          }
        default:
        }

        Threads.sleep(100);
      }
    }

    void stopRunning() {
      running.set(false);
    }

    void rethrowException() throws Exception {
      if (exception.get() != null) {
        throw exception.get();
      }
    }
  }

  static class SimpleClient extends Thread {
    AbstractRpcClient rpcClient;
    AtomicBoolean running = new  AtomicBoolean(true);
    AtomicBoolean sending = new AtomicBoolean(false);
    AtomicReference<Throwable> exception = new AtomicReference<>(null);
    Cluster cluster;
    String id;
    long numCalls = 0;
    Random random = new Random();

    public SimpleClient(Cluster cluster, AbstractRpcClient rpcClient, String id) {
      this.cluster = cluster;
      this.rpcClient = rpcClient;
      this.id = id;
    }

    @Override
    public void run() {
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");

      while (running.get()) {
        boolean isBigPayload = random.nextBoolean();
        String message = isBigPayload ? BIG_PAYLOAD : id + numCalls;
        EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
        EchoResponseProto ret = EchoResponseProto.newBuilder().setMessage("foo").build();

        TestRpcServer server = cluster.getRandomServer();
        try {
          User user = User.getCurrent();
          InetSocketAddress address = server.getListenerAddress();
          if (address == null) {
            throw new IOException("Listener channel is closed");
          }
          sending.set(true);
          ret = (EchoResponseProto)
              rpcClient.callBlockingMethod(md, null, param, ret, user, address);
        } catch (Exception e) {
          LOG.warn(e);
          continue; // expected in case connection is closing or closed
        }

        try {
          assertNotNull(ret);
          assertEquals(message, ret.getMessage());
        } catch (Throwable t) {
          exception.compareAndSet(null, t);
        }

        numCalls++;
      }
    }

    void stopRunning() {
      running.set(false);
    }
    boolean isSending() {
      return sending.get();
    }

    void rethrowException() throws Throwable {
      if (exception.get() != null) {
        throw exception.get();
      }
    }
  }

  /*
  Test that not started connections are successfully removed from connection pool when
  rpc client is closing.
   */
  @Test (timeout = 30000)
  public void testRpcWithWriteThread() throws IOException, InterruptedException {
    LOG.info("Starting test");
    Cluster cluster = new Cluster(1, 1);
    cluster.startServer();
    conf.setBoolean(SPECIFIC_WRITE_THREAD, true);
    for(int i = 0; i <1000; i++) {
      AbstractRpcClient rpcClient = createRpcClient(conf, true);
      SimpleClient client = new SimpleClient(cluster, rpcClient, "Client1");
      client.start();
      while(!client.isSending()) {
        Thread.sleep(1);
      }
      client.stopRunning();
      rpcClient.close();
    }
  }


  @Test (timeout = 900000)
  public void testRpcWithChaosMonkeyWithSyncClient() throws Throwable {
    for (int i = 0; i < numIterations; i++) {
      TimeoutThread.runWithTimeout(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            testRpcWithChaosMonkey(true);
          } catch (Throwable e) {
            if (e instanceof Exception) {
              throw (Exception)e;
            } else {
              throw new Exception(e);
            }
          }
          return null;
        }
      }, 90000);
    }
  }

  @Test (timeout = 900000)
  @Ignore // TODO: test fails with async client
  public void testRpcWithChaosMonkeyWithAsyncClient() throws Throwable {
    for (int i = 0; i < numIterations; i++) {
      TimeoutThread.runWithTimeout(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            testRpcWithChaosMonkey(false);
          } catch (Throwable e) {
            if (e instanceof Exception) {
              throw (Exception)e;
            } else {
              throw new Exception(e);
            }
          }
          return null;
        }
      }, 90000);
    }
  }

  static class TimeoutThread extends Thread {
    long timeout;
    public TimeoutThread(long timeout) {
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
        Threads.printThreadInfo(System.err, "TEST TIMEOUT STACK DUMP");
        System.exit(1); // a timeout happened
      } catch (InterruptedException e) {
        // this is what we want
      }
    }

    // runs in the same thread context but injects a timeout thread which will exit the JVM on
    // timeout
    static void runWithTimeout(Callable<?> callable, long timeout) throws Exception {
      TimeoutThread thread = new TimeoutThread(timeout);
      thread.start();
      callable.call();
      thread.interrupt();
    }
  }

  public void testRpcWithChaosMonkey(boolean isSyncClient) throws Throwable {
    LOG.info("Starting test");
    Cluster cluster = new Cluster(10, 100);
    for (int i = 0; i < 10; i++) {
      cluster.startServer();
    }

    ArrayList<SimpleClient> clients = new ArrayList<>();

    // all threads should share the same rpc client
    AbstractRpcClient rpcClient = createRpcClient(conf, isSyncClient);

    for (int i = 0; i < 30; i++) {
      String clientId = "client_" + i + "_";
      LOG.info("Starting client: " + clientId);
      SimpleClient client = new SimpleClient(cluster, rpcClient, clientId);
      client.start();
      clients.add(client);
    }

    LOG.info("Starting MiniChaosMonkey");
    MiniChaosMonkey cm = new MiniChaosMonkey(cluster);
    cm.start();

    Threads.sleep(30000);

    LOG.info("Stopping MiniChaosMonkey");
    cm.stopRunning();
    cm.join();
    cm.rethrowException();

    LOG.info("Stopping clients");
    for (SimpleClient client : clients) {
      LOG.info("Stopping client: " + client.id);
      LOG.info(client.id + " numCalls:" + client.numCalls);
      client.stopRunning();
      client.join();
      client.rethrowException();
      assertTrue(client.numCalls > 10);
    }

    LOG.info("Stopping RpcClient");
    rpcClient.close();

    LOG.info("Stopping Cluster");
    cluster.stopRunning();
  }
}
