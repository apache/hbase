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

package org.apache.hadoop.hbase.thrift;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A thread pool server customized for HBase.
 */
public class HBaseThreadPoolServer extends TServer {

  private static final String QUEUE_FULL_MSG =
      "Queue is full, closing connection";

  /**
   * This default core pool size should be enough for many test scenarios. We
   * want to override this with a much larger number (e.g. at least 200) for a
   * large-scale production setup.
   */
  public static final int DEFAULT_MIN_WORKER_THREADS = 16;

  public static final int DEFAULT_MAX_WORKER_THREADS = 1000;

  public static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;

  public static final String MIN_WORKER_THREADS_CONF_KEY =
      "hbase.thrift.minWorkerThreads";

  public static final String MAX_WORKER_THREADS_CONF_KEY =
      "hbase.thrift.maxWorkerThreads";

  public static final String MAX_QUEUED_REQUESTS_CONF_KEY =
      "hbase.thrift.maxQueuedRequests";

  private static final Log LOG = LogFactory.getLog(
      HBaseThreadPoolServer.class.getName());

  /**
   * Time to wait after interrupting all worker threads. This is after a clean
   * shutdown has been attempted.
   */
  public static final int SHUTDOWN_NOW_TIME_MS = 5000;

  public static class Options extends TThreadPoolServer.Options {
    public int maxQueuedRequests;

    public Options(Configuration conf) {
      super();
      minWorkerThreads = conf.getInt(MIN_WORKER_THREADS_CONF_KEY,
          DEFAULT_MIN_WORKER_THREADS);
      maxWorkerThreads = conf.getInt(MAX_WORKER_THREADS_CONF_KEY,
          DEFAULT_MAX_WORKER_THREADS);
      maxQueuedRequests = conf.getInt(MAX_QUEUED_REQUESTS_CONF_KEY,
          DEFAULT_MAX_QUEUED_REQUESTS);
    }
  }

  /** Executor service for handling client connections */
  private ExecutorService executorService;

  /** Flag for stopping the server */
  private volatile boolean stopped;

  private Options serverOptions;

  private final int KEEP_ALIVE_TIME_SEC = 60;

  public HBaseThreadPoolServer(TProcessor processor,
      TServerTransport serverTransport,
      TTransportFactory transportFactory,
      TProtocolFactory protocolFactory,
      Options options) {
    super(new TProcessorFactory(processor), serverTransport, transportFactory,
        transportFactory, protocolFactory, protocolFactory);

    BlockingQueue<Runnable> executorQueue;
    if (options.maxQueuedRequests > 0) {
      executorQueue = new LinkedBlockingQueue<Runnable>(
          options.maxQueuedRequests);
    } else {
      executorQueue = new SynchronousQueue<Runnable>();
    }

    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    executorService =
        new ThreadPoolExecutor(options.minWorkerThreads,
            options.maxWorkerThreads, KEEP_ALIVE_TIME_SEC, TimeUnit.SECONDS,
            executorQueue, tfb.build());
    serverOptions = options;
  }

  public void serve() {
    try {
      serverTransport_.listen();
    } catch (TTransportException ttx) {
      LOG.error("Error occurred during listening.", ttx);
      return;
    }

    Runtime.getRuntime().addShutdownHook(new Thread(getClass().getSimpleName() + "-shutdown-hook") {
      @Override
      public void run() {
        HBaseThreadPoolServer.this.stop();
      }
    });

    stopped = false;
    while (!stopped && !Thread.interrupted()) {
      TTransport client = null;
      try {
        client = serverTransport_.accept();
      } catch (TTransportException ttx) {
        if (!stopped) {
          LOG.warn("Transport error when accepting message", ttx);
          continue;
        } else {
          // The server has been stopped
          break;
        }
      }

      ClientConnnection command = new ClientConnnection(client);
      try {
        executorService.execute(command);
      } catch (RejectedExecutionException rex) {
        if (client.getClass() == TSocket.class) {
          // We expect the client to be TSocket.
          LOG.warn(QUEUE_FULL_MSG + " from " +
              ((TSocket) client).getSocket().getRemoteSocketAddress());
        } else {
          LOG.warn(QUEUE_FULL_MSG, rex);
        }
        client.close();
      }
    }

    shutdownServer();
  }

  /**
   * Loop until {@link ExecutorService#awaitTermination} finally does return
   * without an interrupted exception. If we don't do this, then we'll shut
   * down prematurely. We want to let the executor service clear its task
   * queue, closing client sockets appropriately.
   */
  private void shutdownServer() {
    executorService.shutdown();

    long msLeftToWait =
        serverOptions.stopTimeoutUnit.toMillis(serverOptions.stopTimeoutVal);
    long timeMillis = System.currentTimeMillis();

    LOG.info("Waiting for up to " + msLeftToWait + " ms to finish processing" +
        " pending requests");
    boolean interrupted = false;
    while (msLeftToWait >= 0) {
      try {
        executorService.awaitTermination(msLeftToWait, TimeUnit.MILLISECONDS);
        break;
      } catch (InterruptedException ix) {
        long timePassed = System.currentTimeMillis() - timeMillis;
        msLeftToWait -= timePassed;
        timeMillis += timePassed;
        interrupted = true;
      }
    }

    LOG.info("Interrupting all worker threads and waiting for "
        + SHUTDOWN_NOW_TIME_MS + " ms longer");

    // This will interrupt all the threads, even those running a task.
    executorService.shutdownNow();
    Threads.sleepWithoutInterrupt(SHUTDOWN_NOW_TIME_MS);

    // Preserve the interrupted status.
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    LOG.info("Thrift server shutdown complete");
  }

  @Override
  public void stop() {
    stopped = true;
    serverTransport_.interrupt();
  }

  private class ClientConnnection implements Runnable {

    private TTransport client;

    /**
     * Default constructor.
     *
     * @param client Transport to process
     */
    private ClientConnnection(TTransport client) {
      this.client = client;
    }

    /**
     * Loops on processing a client forever
     */
    public void run() {
      TProcessor processor = null;
      TTransport inputTransport = null;
      TTransport outputTransport = null;
      TProtocol inputProtocol = null;
      TProtocol outputProtocol = null;
      try {
        processor = processorFactory_.getProcessor(client);
        inputTransport = inputTransportFactory_.getTransport(client);
        outputTransport = outputTransportFactory_.getTransport(client);
        inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
        // we check stopped_ first to make sure we're not supposed to be shutting
        // down. this is necessary for graceful shutdown.
        while (!stopped && processor.process(inputProtocol, outputProtocol)) {}
      } catch (TTransportException ttx) {
        // Assume the client died and continue silently
      } catch (TException tx) {
        LOG.error("Thrift error occurred during processing of message.", tx);
      } catch (Exception x) {
        LOG.error("Error occurred during processing of message.", x);
      }

      if (inputTransport != null) {
        inputTransport.close();
      }

      if (outputTransport != null) {
        outputTransport.close();
      }
    }
  }
}
