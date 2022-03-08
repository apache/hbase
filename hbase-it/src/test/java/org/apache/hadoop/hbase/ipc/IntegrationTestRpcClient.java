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
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(IntegrationTests.class)
public class IntegrationTestRpcClient {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestRpcClient.class);

  private final Configuration conf;

  private int numIterations = 10;

  public IntegrationTestRpcClient() {
    conf = HBaseConfiguration.create();
  }

  protected AbstractRpcClient<?> createRpcClient(Configuration conf, boolean isSyncClient) {
    return isSyncClient ? new BlockingRpcClient(conf) : new NettyRpcClient(conf) {
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
    ReadWriteLock lock = new ReentrantReadWriteLock();
    HashMap<InetSocketAddress, RpcServer> rpcServers = new HashMap<>();
    List<RpcServer> serverList = new ArrayList<>();
    int maxServers;
    int minServers;

    Cluster(int minServers, int maxServers) {
      this.minServers = minServers;
      this.maxServers = maxServers;
    }

    RpcServer startServer() throws IOException {
      lock.writeLock().lock();
      try {
        if (rpcServers.size() >= maxServers) {
          return null;
        }

        RpcServer rpcServer = RpcServerFactory.createRpcServer(null,
            "testRpcServer", Lists
                .newArrayList(new BlockingServiceAndInterface(SERVICE, null)),
            new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(
                conf, 1));
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
      RpcServer rpcServer = null;
      try {
        if (rpcServers.size() <= minServers) {
          return;
        }
        int size = rpcServers.size();
        int rand = ThreadLocalRandom.current().nextInt(size);
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

    void stopServer(RpcServer rpcServer) throws InterruptedException {
      InetSocketAddress address = rpcServer.getListenerAddress();
      LOG.info("Stopping server: " + address);
      rpcServer.stop();
      rpcServer.join();
      LOG.info("Stopped server: " + address);
    }

    void stopRunning() throws InterruptedException {
      lock.writeLock().lock();
      try {
        for (RpcServer rpcServer : serverList) {
          stopServer(rpcServer);
        }

      } finally {
        lock.writeLock().unlock();
      }
    }

    RpcServer getRandomServer() {
      lock.readLock().lock();
      try {
        int size = rpcServers.size();
        int rand = ThreadLocalRandom.current().nextInt(size);
        return serverList.get(rand);
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  static class MiniChaosMonkey extends Thread {
    AtomicBoolean running = new  AtomicBoolean(true);
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    Cluster cluster;

    public MiniChaosMonkey(Cluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public void run() {
      while (running.get()) {
        if (ThreadLocalRandom.current().nextBoolean()) {
          //start a server
          try {
            cluster.startServer();
          } catch (Exception e) {
            LOG.warn(e.toString(), e);
            exception.compareAndSet(null, e);
          }
        } else {
          // stop a server
          try {
            cluster.stopRandomServer();
          } catch (Exception e) {
            LOG.warn(e.toString(), e);
            exception.compareAndSet(null, e);
          }
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
    AbstractRpcClient<?> rpcClient;
    AtomicBoolean running = new  AtomicBoolean(true);
    AtomicBoolean sending = new AtomicBoolean(false);
    AtomicReference<Throwable> exception = new AtomicReference<>(null);
    Cluster cluster;
    String id;
    long numCalls = 0;

    public SimpleClient(Cluster cluster, AbstractRpcClient<?> rpcClient, String id) {
      this.cluster = cluster;
      this.rpcClient = rpcClient;
      this.id = id;
      this.setName(id);
    }

    @Override
    public void run() {
      while (running.get()) {
        boolean isBigPayload = ThreadLocalRandom.current().nextBoolean();
        String message = isBigPayload ? BIG_PAYLOAD : id + numCalls;
        EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
        EchoResponseProto ret;
        RpcServer server = cluster.getRandomServer();
        try {
          sending.set(true);
          BlockingInterface stub = newBlockingStub(rpcClient, server.getListenerAddress());
          ret = stub.echo(null, param);
        } catch (Exception e) {
          LOG.warn(e.toString(), e);
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
  @Test
  public void testRpcWithWriteThread() throws IOException, InterruptedException {
    LOG.info("Starting test");
    Cluster cluster = new Cluster(1, 1);
    cluster.startServer();
    conf.setBoolean(SPECIFIC_WRITE_THREAD, true);
    for(int i = 0; i <1000; i++) {
      AbstractRpcClient<?> rpcClient = createRpcClient(conf, true);
      SimpleClient client = new SimpleClient(cluster, rpcClient, "Client1");
      client.start();
      while(!client.isSending()) {
        Thread.sleep(1);
      }
      client.stopRunning();
      rpcClient.close();
    }
  }


  @Test
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
      }, 180000);
    }
  }

  @Test
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

    ArrayList<SimpleClient> clients = new ArrayList<>(30);

    // all threads should share the same rpc client
    AbstractRpcClient<?> rpcClient = createRpcClient(conf, isSyncClient);

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
