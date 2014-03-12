/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.throttles.SizeBasedMultiThrottler;
import org.apache.hadoop.hbase.util.throttles.SizeBasedThrottler;
import org.apache.hadoop.hbase.util.throttles.SizeBasedThrottlerInterface;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 *
 * <p>Copied local so can fix HBASE-900.
 *
 * @see HBaseClient
 */
public abstract class HBaseServer {

  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());

  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : RPC was refactored in 0.19
  public static final byte VERSION_3 = 3;
  // 4 : RPC options object in RPC protocol with compression,
  //     profiling, and tagging
  public static final byte VERSION_RPCOPTIONS = 4;

  public static final byte CURRENT_VERSION = VERSION_RPCOPTIONS;

  private SizeBasedThrottlerInterface callQueueThrottler;

  public static final Log LOG = LogFactory.getLog(HBaseServer.class.getName());

  protected static final ThreadLocal<HBaseServer> SERVER =
    new ThreadLocal<HBaseServer>();

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.
   * @return HBaseServer
   */
  public static HBaseServer get() {
    return SERVER.get();
  }

  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   *  @return String
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /**
   * Stub method for getting information about a RPC call. In the future, we
   *  could use this for real pretty printing of non-rpc calls.
   * @see HBaseRPC#Server#getParamFormatMap(java.lang.reflect.Method, Object[])
   * @param method Ignored for now
   * @param params Ignored for now
   * @return null
   */
  public Map<String, Object> getParamFormatMap(Method method, Object[] params) {
    return null;
  }

  protected String bindAddress;
  protected int port;                             // port we listen on
  private int handlerCount;                       // number of handler threads
  protected Class<? extends Writable> paramClass; // class of call parameters
  protected int maxIdleTime;                      // the maximum idle time after
                                                  // which a client may be
                                                  // disconnected
  protected int thresholdIdleConnections;         // the number of idle
                                                  // connections after which we
                                                  // will start cleaning up idle
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of
                                                  // connections to nuke
                                                  // during a cleanup

  protected HBaseRpcMetrics  rpcMetrics;

  protected Configuration conf;

  protected int socketSendBufferSize;
  protected final boolean tcpNoDelay;   // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

  // responseQueuesSizeThrottler is shared among all responseQueues,
  // it bounds memory occupied by responses in all responseQueues
  final SizeBasedThrottler responseQueuesSizeThrottler;

  // RESPONSE_QUEUE_MAX_SIZE limits total size of responses in every response queue
  private static final long DEFAULT_RESPONSE_QUEUES_MAX_SIZE = 1024 * 1024 * 1024; // 1G
  private static final String RESPONSE_QUEUES_MAX_SIZE = "ipc.server.response.queue.maxsize";

  volatile protected boolean running = true;         // true while server runs
  protected BlockingQueue<RawCall> callQueue; // queued calls

  protected final List<Connection> connectionList =
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  protected Responder responder = null;
  protected int numConnections = 0;
  private Handler[] handlers = null;
  protected HBaseRPCErrorHandler errorHandler = null;

  /**
   * A convenience method to bind to a given address and report
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException =
        new BindException("Problem binding to " + address + " : " +
            e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " +
                                       address.getHostName());
      }
      throw e;
    }
  }

  /** A call queued for handling. */
  @ThriftStruct
  public static class Call {
    protected int id;                             // the client's call id
    protected Writable param;                     // the parameter passed
    protected Connection connection;              // connection to client
    protected long timestamp;      // the time received when response is null
                                   // the time served when response is not null
    protected ByteBuffer response;                // the response for this call
    protected Compression.Algorithm compressionAlgo =
      Compression.Algorithm.NONE;
    protected int version = CURRENT_VERSION;     // version used for the call

    protected boolean shouldProfile = false;
    protected ProfilingData profilingData = null;
    protected String tag = null;
    protected long partialResponseSize; // size of the results collected so far

    public Call(int id, Writable param, Connection connection, long timestamp) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.response = null;
      this.partialResponseSize = 0;
      this.timestamp = timestamp;
    }

    /**
     * Thrift constructor
     * TODO: serialize more fields, this is just for trial
     * @param id
     * @param partialResponseSize
     * @param shouldProfile
     */
    @ThriftConstructor
    public Call(@ThriftField(1) int id,
        @ThriftField(2) long partialResponseSize,
        @ThriftField(3) boolean shouldProfile,
        @ThriftField(4) ByteBuffer response,
        @ThriftField(5) long timestamp,
        @ThriftField(6) ProfilingData profilingData){
      this.id = id;
      this.partialResponseSize = partialResponseSize;
      this.shouldProfile = shouldProfile;
      this.response = response;
      this.timestamp = timestamp;
      this.profilingData = profilingData;
    }

    /**
     * This Constructor should be used when the Client sends header to the
     * Server and in pratice we just need to know whether the call need to be
     * profiled or not
     *
     * @param options
     */
    public Call(HBaseRPCOptions options) {
      this.shouldProfile = options.getRequestProfiling();
    }

    @ThriftField(1)
    public int getId() {
      return id;
    }

    @ThriftField(5)
    public long getTimestamp() {
      return timestamp;
    }

    @ThriftField(4)
    public ByteBuffer getResponse() {
      return response;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public String getTag() {
      return tag;
    }

    public void setVersion(int version) {
     this.version = version;
    }

    public int getVersion() {
      return version;
    }

    public void setRPCCompression(Compression.Algorithm compressionAlgo) {
      this.compressionAlgo = compressionAlgo;
    }

    public Compression.Algorithm getRPCCompression() {
      return this.compressionAlgo;
    }

    @ThriftField(2)
    public long getPartialResponseSize() {
      return partialResponseSize;
    }

    public void setPartialResponseSize(long partialResponseSize) {
      this.partialResponseSize = partialResponseSize;
    }

    public Connection getConnection() {
      return this.connection;
    }

    public void setResponse(ByteBuffer response) {
      this.response = response;
    }

    @ThriftField(6)
    public ProfilingData getProfilingData(){
      return this.profilingData;
    }

    @ThriftField(3)
    public boolean isShouldProfile() {
      return shouldProfile;
    }

    public void setShouldProfile(boolean shouldProfile) {
      this.shouldProfile = shouldProfile;
    }

    public void setProfilingData(ProfilingData profilingData) {
      this.profilingData = profilingData;
    }

    @Override
    public String toString() {
      return "Call [id=" + id + ", param=" + param + ", connection="
          + connection + ", timestamp=" + timestamp + ", response=" + response
          + ", compressionAlgo=" + compressionAlgo + ", version=" + version
          + ", shouldProfile=" + shouldProfile + ", profilingData="
          + profilingData + ", tag=" + tag + ", partialResponseSize="
          + partialResponseSize + "]";
    }
  }

  /** Listens on the socket, accepts new connections and handles them to readers*/
  private class Listener extends HasThread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between
                                          //two cleanup runs
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);

    private final int readerCount = conf.getInt("ipc.server.reader.count",
        Runtime.getRuntime().availableProcessors() + 1); // number of reader threads

    /* guarded by lock on this */
    private final Reader[] readers;

    /* guarded by lock on this */
    private int readerRRIndex = 0; // New connections are passed to readers round-robin way.

    public Listener() throws IOException {
      // this will trigger a DNS lookup
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);

      readers = new Reader[readerCount];
      for (int i = 0; i < readerCount; i++) {
        readers[i] = new Reader(i);
      }
    }

    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all
     * connections will be looked at for the cleanup.
     * @param force all connections will be looked at for cleanup
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isTraceEnabled())
              LOG.trace(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            //noinspection UnusedAssignment
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(HBaseServer.this);

      while (running) {
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext() && running) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable()) {
                  doAccept(key);
                } else {
                  LOG.warn("Woken on not acceptable channel");
                }
              }
            } catch (IOException ignored) {
            }
            key = null;
          }
        } catch (IOException e) {
          LOG.error("Exception caught: " + e.toString());
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) { }

        selector= null;
        acceptChannel= null;

        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }

    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
      Connection c;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      // accept up to 10 connections
      for (int i = 0; i < 10; i++) {
        SocketChannel channel = server.accept();
        if (channel==null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(tcpKeepAlive);
        c = new Connection(channel, System.currentTimeMillis());
        getReader().addConnection(c);
        synchronized (connectionList) {
          connectionList.add(numConnections, c);
          numConnections++;
        }
        if (LOG.isTraceEnabled())
          LOG.trace("Server connection from " + c.toString() +
              "; # active connections: " + numConnections +
              "; # queued calls: " + callQueue.size());
      }
    }

    private synchronized Reader getReader() {
      if (readerRRIndex == readerCount) {
        readerRRIndex = 0;
      }

      return readers[readerRRIndex++];
    }

    @Override
    public synchronized void start(){
      for (int i = 0; i < readerCount; i++) {
        readers[i].start();
      }

      super.start();
    }

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.warn(getName() + ":Exception in closing listener socket. " + e);
        }
      }

      if (readers != null) {
        for (Reader reader : readers) {
          if (reader != null) {
            try{
              reader.stop();
            } catch (IOException e) {
              LOG.warn("Caught: " + e.toString());
            }
          }
        }
      }
    }
  }


  /**
   * Reader is a thread that reads from connections.
   * Listener thread hands accepted connections over to Readers.
   * Every Reader has its own set of connections to read from and
   * Listener makes sure, that one connection is handled exactly by
   * one Reader
   */
  private class Reader extends HasThread {

    private static final int CONNECTION_QUEUE_SIZE = 10;

    private final Selector selector;

    /*
     * This is a list of new connections. When Listener signals that Reader
     * should start reading from new connection, this connection is added to
     * newConnections list. All connections from this list are periodically
     * registered with Reader's selector and then this list is cleared.
     */
    private final List<Connection> newConnections;

    /*
     * thread id for logging etc
     */
    private final int threadId;

    public Reader(int threadId) throws IOException {
      this.newConnections = new ArrayList<Connection>(CONNECTION_QUEUE_SIZE);
      this.selector = Selector.open();
      this.threadId = threadId;

      this.setDaemon(true);
      this.setName(String.format("Reader %d", threadId));
    }

    public void addConnection(Connection connection) {
      if (running){
        synchronized (newConnections) {
          newConnections.add(connection);
        }
        selector.wakeup();
      }
    }

    private void registerNewConnections() {
      synchronized (newConnections) {
        if (newConnections.isEmpty()) {
          return;
        }

        for (Connection conn : newConnections) {
          try {
            SelectionKey readKey = conn.channel.register(selector, SelectionKey.OP_READ);
            readKey.attach(conn);
          } catch (ClosedChannelException e) {
            // It is ok.
          }
        }
        newConnections.clear();
      }
    }

    public void stop() throws IOException{
      selector.close();
      interrupt();
    }

    private void processKey(SelectionKey key) throws InterruptedException {
      if (key.isValid()) {
        if (key.isReadable()) {
          doRead(key);
        } else {
          LOG.warn(String.format("Reader %d woken up on nonreadable connection.", threadId));
        }
      }
    }

    private void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());

      try {
        count = c.readAndQueue();
      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        if (count > 0) {
          LOG.warn(getName() + ": readAndProcess threw exception " + e +
              ". Count of bytes read: " + count, e);
        }
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isTraceEnabled())
          LOG.trace(getName() + ": disconnecting client " +
                    c.getHostAddress() + ". Number of active connections: "+
                    numConnections);
        closeConnection(c);
        // c = null;
      }
      else {
        c.setLastContact(System.currentTimeMillis());
      }
    }

    @Override
    public void run() {
      LOG.info(this.getName() + " started");
      SelectionKey key = null;
      while(running) {
        try{
          int count = selector.select();
          if (count > 0) {
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
              key = keys.next();
              keys.remove();
              processKey(key);
            }
          }

          registerNewConnections();
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              closeCurrentConnection(key);
              running = false;
              listener.selector.wakeup();
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn("Out of Memory in server select", e);
            closeCurrentConnection(key);
            listener.cleanupConnections(true);
            try { sleep(60000); } catch (InterruptedException ignored) {}
          }
        } catch (Exception e) {
          closeCurrentConnection(key);
        }
      }

      LOG.info(this.getName() + " finished");
    }

    private void closeCurrentConnection(SelectionKey key) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isTraceEnabled())
            LOG.trace(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
        }
      }
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends HasThread {
    private Selector writeSelector;
    private int pending;         // connections waiting to register

    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(HBaseServer.this);
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.warn(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if (LOG.isTraceEnabled()) {
            LOG.trace("Checking for old call responses.");
          }
          ArrayList<Call> calls;

          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) {
                calls.add(call);
              }
            }
          }

          for(Call call : calls) {
            doPurge(call, now);
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn("Out of Memory in server select", e);
            try { Thread.sleep(60000); } catch (Exception ignored) {}
      }
        } catch (Exception e) {
          LOG.warn("Exception in Responder " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue
    // for a long time.
    //
    private void doPurge(Call call, long now) {
      synchronized (call.connection.responseQueue) {
        Iterator<Call> iter = call.connection.responseQueue.listIterator(0);
        while (iter.hasNext()) {
          Call nextCall = iter.next();
          if (now > nextCall.timestamp + PURGE_INTERVAL) {
            closeConnection(nextCall.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(final LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements;
      Call call = null;
      try {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.peek();
          SocketChannel channel = call.connection.channel;
          if (LOG.isTraceEnabled()) {
            LOG.trace(getName() + ": responding to #" + call.id + " from " +
                      call.connection);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            // Error flag is set, so returning here closes connection and
            // clears responseQueue.
            return true;
          }
          if (!call.response.hasRemaining()) {
            responseQueue.poll();
            responseQueuesSizeThrottler.decrease(call.response.limit());
            call.connection.decRpcCount();
            //noinspection RedundantIfStatement
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = System.currentTimeMillis();

              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote partial " + numBytes +
                        " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException, InterruptedException {
      boolean closed;
      responseQueuesSizeThrottler.increase(call.response.remaining());
      synchronized (call.connection.responseQueue) {
        closed = call.connection.closed;
        if (!closed) {
          call.connection.responseQueue.addLast(call);
          if (call.connection.responseQueue.size() == 1) {
            processResponse(call.connection.responseQueue, true);
          }
        }
      }
      if (closed) {
        // Connection was closed when we tried to submit response, but we
        // increased responseQueues size already. It shoud be
        // decreased here.
        responseQueuesSizeThrottler.decrease(call.response.remaining());
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  /**
   * This class is used to delay parsing of incomming requests. It is a storage for
   * ByteBuffer and timestamp. To parse raw ByteBuffer to get Call object with timestamp
   * equal to read time, just call parse method.
   */
  private class RawCall {
    private final ByteBuffer data;
    private final Connection connection;
    private final long timestamp; // When RawCall was created;

    public RawCall(Connection connection, ByteBuffer data){
      this.connection = connection;
      this.data = data;
      this.timestamp = System.currentTimeMillis();
    }

    public Call parse() throws IOException{
      DataInputStream uncompressedIs =
          new DataInputStream(new ByteArrayInputStream(data.array()));
      Compression.Algorithm txCompression = Algorithm.NONE;
      Compression.Algorithm rxCompression = Algorithm.NONE;
      DataInputStream dis = uncompressedIs;

      // 1. read the call id uncompressed
      int id = uncompressedIs.readInt();
        if (LOG.isTraceEnabled())
          LOG.trace(" got #" + id);

        HBaseRPCOptions options = new HBaseRPCOptions ();
        Decompressor decompressor = null;
        if (connection.version >= VERSION_RPCOPTIONS) {
          // 2. read rpc options uncompressed
          options.readFields(dis);
          txCompression = options.getTxCompression();   // server receives this
          rxCompression = options.getRxCompression();   // server responds with
          // 3. set up a decompressor to read the rest of the request
          if (txCompression != Compression.Algorithm.NONE) {
            decompressor = txCompression.getDecompressor();
            InputStream is = txCompression.createDecompressionStream(
                uncompressedIs, decompressor, 0);
            dis = new DataInputStream(is);
          }
        }
        // 4. read the rest of the params
        Writable param = ReflectionUtils.newInstance(paramClass, conf);
        param.readFields(dis);

        Call call = new Call(id, param, connection, timestamp);
        call.shouldProfile = options.getRequestProfiling ();

        call.setRPCCompression(rxCompression);
        call.setVersion(connection.version);
        call.setTag(options.getTag());

        if (decompressor != null) {
          txCompression.returnDecompressor(decompressor);
        }

        return call;
    }
  }


  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private boolean versionRead = false; //if initial signature and
                                         //version are read
    private int version = -1;
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.

    protected volatile boolean closed = false;    // indicates if connection was closed
    protected SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    protected Socket socket;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    protected UserGroupInformation ticket = null;

    public Connection(SocketChannel channel, long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    public Socket getSocket() {
      return socket;
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getRemotePort() {
      return remotePort;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount--;
    }

    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    public int readAndQueue() throws IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */
        int count;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0)
            return count;
        }

        if (!versionRead) {
          //Every connection is expected to send the header.
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channelRead(channel, versionBuffer);
          if (count <= 0) {
            return count;
          }
          version = versionBuffer.get(0);

          dataLengthBuffer.flip();
          if (!HEADER.equals(dataLengthBuffer) ||
              version < VERSION_3 || version > CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " +
                     hostAddress + ":" + remotePort +
                     " got header " + dataLengthBuffer +
                     ", version " + version +
                     " supported versions [" + VERSION_3 +
                     " ... " + CURRENT_VERSION + "]");
            return -1;
          }
          dataLengthBuffer.clear();
          versionRead = true;
          continue;
        }

        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();

          if (dataLength == HBaseClient.PING_CALL_ID) {
            dataLengthBuffer.clear();
            return 0;  //ping message
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount();  // Increment the rpc count
        }

        count = channelRead(channel, data);

        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (headerRead) {
            queueRawCall();
            data = null;
            return count;
          }
          processHeader();
          headerRead = true;
          data = null;
          continue;
        }
        return count;
      }
    }

    private void queueRawCall() throws InterruptedException {
      // To ensure that the received timestamp is set
      // accurately create the call before the throttler.
      RawCall call = new RawCall(this, data);
      callQueueThrottler.increase(data.limit());
      try {
        // queue the call
        callQueue.put(call);
      } catch (InterruptedException e)  {
        callQueueThrottler.decrease(data.limit());
      }
    }

    /// Reads the header following version
    private void processHeader() throws IOException {
      /* In the current version, it is just a ticket.
       * Later we could introduce a "ConnectionHeader" class.
       */
      DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      ticket = (UserGroupInformation) ObjectWritable.readObject(in, conf);
    }

    protected synchronized void close() {
      closed = true;
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception ignored) {} // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {socket.close();} catch(Exception ignored) {}
    }
  }

  /** Handles queued calls . */
  private class Handler extends HasThread {
    static final int BUFFER_INITIAL_SIZE = 1024;
    private MonitoredRPCHandler status;

    public Handler(int instanceNumber) {
      this.setDaemon(true);
      String name = "IPC Server handler "+ instanceNumber + " on " + port;
      this.setName(name);
      this.status = TaskMonitor.get().createRPCStatus(name);
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      status.setStatus("starting");
      status.setProcessingServer(HBaseServer.this);
      SERVER.set(HBaseServer.this);
      while (running) {
        try {
          status.pause("Waiting for a call");
          RawCall rawCall = callQueue.take(); // pop the queue; maybe blocked here
          callQueueThrottler.decrease(rawCall.data.limit());
          Call call = rawCall.parse();
          status.setStatus("Setting up call");
          status.setConnection(call.connection.getHostAddress(),
              call.connection.getRemotePort());

          if (LOG.isTraceEnabled())
            LOG.trace(getName() + ": has #" + call.id + " from " + call.connection);

          String errorClass = null;
          String error = null;
          Writable value = null;

          if (HRegionServer.enableServerSideProfilingForAllCalls.get()
              || call.shouldProfile) {
            call.profilingData = new ProfilingData ();
          } else {
            call.profilingData = null;
          }
          HRegionServer.callContext.set(call);

          CurCall.set(call);
          UserGroupInformation previous = UserGroupInformation.getCurrentUGI();
          UserGroupInformation.setCurrentUser(call.connection.ticket);
          long start = System.currentTimeMillis ();
          try {
            // make the call
            value = call(call.param, call.timestamp, status);
          } catch (Throwable e) {
            LOG.warn(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          long total = System.currentTimeMillis () - start;
          UserGroupInformation.setCurrentUser(previous);
          CurCall.set(null);

          if (HRegionServer.enableServerSideProfilingForAllCalls.get()
              || call.shouldProfile) {
            call.profilingData.addLong(
                ProfilingData.TOTAL_SERVER_TIME_MS, total);
          }
          HRegionServer.callContext.remove();

          int size = BUFFER_INITIAL_SIZE;
          if (value instanceof WritableWithSize) {
            // get the size hint.
            WritableWithSize ohint = (WritableWithSize)value;
            long hint = ohint.getWritableSize();
            if (hint > 0) {
              hint = hint + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT;
              if (hint > Integer.MAX_VALUE) {
                // oops, new problem.
                IOException ioe =
                    new IOException("Result buffer size too large: " + hint);
                errorClass = ioe.getClass().getName();
                error = StringUtils.stringifyException(ioe);
              } else {
                size = ((int)hint);
              }
            }
          }
          ByteBufferOutputStream buf = new ByteBufferOutputStream(size);
          DataOutputStream rawOS = new DataOutputStream(buf);
          DataOutputStream out = rawOS;
          Compressor compressor = null;

          // 1. write call id uncompressed
          out.writeInt(call.id);

          // 2. write error flag uncompressed
          out.writeBoolean(error != null);

          if (call.getVersion() >= VERSION_RPCOPTIONS) {
            // 3. write the compression type for the rest of the response
            out.writeUTF(call.getRPCCompression().getName());

            // 4. create a compressed output stream if compression was enabled
            if (call.getRPCCompression() != Compression.Algorithm.NONE) {
              compressor = call.getRPCCompression().getCompressor();
              OutputStream compressedOutputStream =
                call.getRPCCompression().createCompressionStream(rawOS, compressor, 0);
              out = new DataOutputStream(compressedOutputStream);
            }
          }

          // 5. write the output as per the compression
          if (error == null) {
            value.write(out);
            // write profiling data if requested
            if (call.getVersion () >= VERSION_RPCOPTIONS) {
              if (!call.shouldProfile) {
                out.writeBoolean(false);
              } else {
                out.writeBoolean(true);
                call.profilingData.write(out);
              }
            }
          } else {
            WritableUtils.writeString(out, errorClass);
            WritableUtils.writeString(out, error);
          }

          out.flush();
          buf.flush();
          call.setResponse(buf.getByteBuffer());
          responder.doRespond(call);
          if (compressor != null) {
            call.getRPCCompression().returnCompressor(compressor);
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.warn(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.error(getName() + ": exiting on OOME");
              return;
            }
          } else {
            // rethrow if no handler
            throw e;
          }
        } catch (Exception e) {
          LOG.warn(getName() + " caught: " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }
  }

  protected HBaseServer(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount,
                  Configuration conf)
    throws IOException
  {
    this(bindAddress, port, paramClass, handlerCount,  conf, Integer.toString(port));
  }
  /* Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   *
   */
  protected HBaseServer(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount,
                  Configuration conf, String serverName)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.callQueue  = new LinkedBlockingQueue<RawCall>();
    long maxCallQueueMemorySize = conf.getLong(
        HConstants.MAX_CALL_QUEUE_MEMORY_SIZE_STRING,
        HConstants.MAX_CALL_QUEUE_MEMORY_SIZE);
    long maxSmallerCallQueueMemorySize = conf.getLong(
        HConstants.MAX_SMALLER_CALL_QUEUE_MEMORY_SIZE_STRING,
        HConstants.MAX_SMALLER_CALL_QUEUE_MEMORY_SIZE);
    long maxLargerCallQueueMemorySize = conf.getLong(
        HConstants.MAX_LARGER_CALL_QUEUE_MEMORY_SIZE_STRING,
        HConstants.MAX_LARGER_CALL_QUEUE_MEMORY_SIZE);
    int smallQueueRequestLimit = conf.getInt(
        HConstants.SMALL_QUEUE_REQUEST_LIMIT_STRING,
        HConstants.SMALL_QUEUE_REQUEST_LIMIT);
    if (conf.getBoolean(HConstants.USE_MULTIPLE_THROTTLES, false)) {
      callQueueThrottler = new SizeBasedMultiThrottler(
          maxSmallerCallQueueMemorySize,
          maxLargerCallQueueMemorySize,
          smallQueueRequestLimit);
    } else {
      callQueueThrottler = new SizeBasedThrottler(maxCallQueueMemorySize);
    }
    this.maxIdleTime = 2*conf.getInt("ipc.client.connection.maxidletime", 1000);
    this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold", 4000);

    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();
    this.rpcMetrics = new HBaseRpcMetrics(serverName,
                          Integer.toString(this.port));
    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
    this.tcpKeepAlive = conf.getBoolean("ipc.server.tcpkeepalive", true);

    this.responseQueuesSizeThrottler = new SizeBasedThrottler(
        conf.getLong(RESPONSE_QUEUES_MAX_SIZE, DEFAULT_RESPONSE_QUEUES_MAX_SIZE));

    // Create the responder here
    responder = new Responder();
  }

  protected void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    connection.close();

    long bytes = 0;
    synchronized (connection.responseQueue) {
      for (Call c : connection.responseQueue) {
        bytes += c.response.limit();
      }
      connection.responseQueue.clear();
    }
    responseQueuesSizeThrottler.decrease(bytes);
  }

  /** Sets the socket buffer size used for responding to RPCs.
   * @param size send size
   */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];

    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /** Called for each call.
   * @param param writable parameter
   * @param receiveTime time
   * @param status The task monitor for the associated handler.
   * @return Writable
   * @throws IOException e
   */
  public abstract Writable call(Writable param, long receiveTime,
      MonitoredRPCHandler status) throws IOException;

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return numConnections;
  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 64*1024; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @param channel writable byte channel to write to
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see WritableByteChannel#write(ByteBuffer)
   */
  protected static int channelWrite(WritableByteChannel channel,
                                    ByteBuffer buffer) throws IOException {
    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.write(buffer) : channelIO(null, channel, buffer);
  }

  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  protected static int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {
    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
  }

  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }

  public long getResponseQueueSize(){
    return responseQueuesSizeThrottler.getCurrentValue();
  }
}
