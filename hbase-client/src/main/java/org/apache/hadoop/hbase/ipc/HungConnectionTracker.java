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
package org.apache.hadoop.hbase.ipc;

import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HubSpot modification: This class tracks threads writing to an IPC connection and attempts to
 * manually interrupt them if they significantly overshoot their call timeout. This class relies on
 * the fact that track and complete are called within the RpcClient outLock. This ensure that we are
 * only every tracking one thread per IPC client. We only open a single IPC client to each region
 * server, so the number of total threads tracked at a single time should never exceed the total
 * number of region servers the client is talking to. Note: this class is only necessary because of
 * how we inject SSLSocketFactory into the underlying net code of the client. This was necessary for
 * our haproxy SSL solution, but once we finish the hbase2 client upgrade we should be able to
 * migrate onto native TLS and NettyRpcConnection. At that point we can delete this class and
 * related methods.
 */
@InterfaceAudience.Private
public class HungConnectionTracker {

  private static final long CHECK_INTERVAL_MILLIS = 700;
  private static final Logger LOG = LoggerFactory.getLogger(HungConnectionTracker.class);
  private static final long INTERRUPT_BUFFER_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(1_000);
  private static final String[] SUPPORTED_JAVA_VERSIONS = { "11", "17" };
  private static boolean IS_TARGET_JAVA_VERSION;
  static {
    for (String v : SUPPORTED_JAVA_VERSIONS) {
      if (System.getProperty("java.specification.version").startsWith(v)) {
        IS_TARGET_JAVA_VERSION = true;
      }
    }
  }

  public static final LongAdder INTERRUPTED = new LongAdder();

  private final ConcurrentHashMap<Thread, ConnectionData> trackedThreads;
  private final Thread interrupterThread;

  private Class<?> sslSocketImpl;
  private Method socketCloseMethod;

  public HungConnectionTracker() {
    this.trackedThreads = new ConcurrentHashMap<>();
    this.interrupterThread =
      new Thread(this::processTrackedThreads, "hbase-hung-connection-interrupter");
    this.interrupterThread.setDaemon(true);
    this.interrupterThread.start();

    // Using the normal `close` method on java SSL sockets attempts to send a `close_notify`. This
    // causes our close to keep hanging since we are detecting when the network connection is
    // impaired.
    // We use reflection to access the raw close socket method and forcibly close the connection to
    // the hbase server without a `close_notify`.
    try {
      this.sslSocketImpl = Class.forName("sun.security.ssl.SSLSocketImpl");
      this.socketCloseMethod = sslSocketImpl.getDeclaredMethod("closeSocket", boolean.class);
      this.socketCloseMethod.setAccessible(true);
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      LOG.info("Couldn't find SSLSocketImpl.closeSocket(boolean) method", e);
      this.sslSocketImpl = null;
      this.socketCloseMethod = null;
    }

    // Since we are using reflection on an internal method, it's expected it could change from
    // version to version. This ensures we don't see a strange impact
    // on a major java version upgrade if the function keeps the same name but has a different
    // impact.
    if (!IS_TARGET_JAVA_VERSION) {
      LOG.info(
        "Current java version does not matched desired version, skipping reflection based close");
      this.sslSocketImpl = null;
      this.socketCloseMethod = null;
    }
  }

  public void track(Thread thread, Call call, Address address, Socket socket) {
    trackedThreads.put(thread, new ConnectionData(System.nanoTime(), call, address, socket));
  }

  public boolean complete(Thread thread) {
    ConnectionData val = trackedThreads.remove(thread);
    return val == null || val.isInterrupted();
  }

  private void processTrackedThreads() {
    while (true) {
      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        LOG.info("Interrupted", e);
      }

      try {
        for (Entry<Thread, ConnectionData> connectionEntry : trackedThreads.entrySet()) {
          ConnectionData connectionData = connectionEntry.getValue();
          Thread hungThread = connectionEntry.getKey();
          long durationNanos = System.nanoTime() - connectionData.getStartTimeNanos();

          if (durationNanos > connectionData.getRemainingTimeNanos()) {
            LOG.info(
              "Detected hung connection on thread={} callId={} to server={} executingFor={} ms. Attempting to close the socket and interrupt the thread.",
              hungThread.getName(), connectionData.getCall().id, connectionData.getServerName(),
              TimeUnit.NANOSECONDS.toMillis(durationNanos));

            try {
              connectionData.getSocket().setSoLinger(true, 0);

              // Our SSL sockets don't work will when the upstream has closed. We use reflection to
              // force the underlying socket to close faster.
              if (
                socketCloseMethod != null && sslSocketImpl.isInstance(connectionData.getSocket())
              ) {
                LOG.debug("Invoking SSLSocketImpl.closeSocket(boolean)");
                socketCloseMethod.invoke(connectionData.getSocket(), false);
              } else {
                LOG.debug("Invoking Socket.close");
                connectionData.getSocket().close();
              }

              LOG.debug("Interrupting thread {}", hungThread.getName());
              INTERRUPTED.increment();
              hungThread.interrupt();
              connectionData.setInterrupted(true);
            } finally {
              complete(hungThread);
            }
          }
        }
      } catch (Exception e) {
        LOG.info("Failed to process threads", e);
      }
    }
  }

  private final class ConnectionData {
    private final long startTimeNanos;
    private final Call call;
    private final long remainingTimeNanos;
    private final Address address;
    private final Socket socket;
    private volatile boolean interrupted;

    private ConnectionData(long startTimeNanos, Call call, Address address, Socket socket) {
      this.startTimeNanos = startTimeNanos;
      this.call = call;
      this.remainingTimeNanos =
        TimeUnit.MILLISECONDS.toNanos(call.getRemainingTime()) + INTERRUPT_BUFFER_WINDOW_NANOS;
      this.address = address;
      this.socket = socket;
    }

    public long getStartTimeNanos() {
      return startTimeNanos;
    }

    public Call getCall() {
      return call;
    }

    public long getRemainingTimeNanos() {
      return remainingTimeNanos;
    }

    public String getServerName() {
      return address.toString();
    }

    public Socket getSocket() {
      return socket;
    }

    public void setInterrupted(boolean interrupted) {
      this.interrupted = interrupted;
    }

    public boolean isInterrupted() {
      return interrupted;
    }
  }
}
