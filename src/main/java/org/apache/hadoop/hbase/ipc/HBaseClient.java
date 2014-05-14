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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * <p>This is the org.apache.hadoop.ipc.Client renamed as HBaseClient and
 * moved into this package so can access package-private methods.
 *
 * @see HBaseServer
 */
public class HBaseClient {

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseClient");
  // Active connections are stored in connections.
  protected final ConcurrentMap<ConnectionId, Connection> connections = 
    new ConcurrentHashMap<ConnectionId, Connection>();
  
  protected int counter;                            // counter for call ids
  protected final AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final protected Configuration conf;
  final protected int maxIdleTime; // connections will be culled if it was idle for
                           // maxIdleTime microsecs
  final protected int maxConnectRetries; //the max. no. of retries for socket connections
  final protected long failureSleep; // Time to sleep before retry on failure.
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final int ipTosValue; // specify a datagram's type-of-service priority
  protected int pingInterval; // how often sends ping to the server in msecs
  private final int connectionTimeOutMillSec; // the connection time out

  private final int numConnectionsPerServer;
  public static final String NUM_CONNECTIONS_PER_SERVER = 
    "hbase.client.max.connections.per.server";
  public static final int DEFAULT_NUM_CONNECTIONS_PER_SERVER = 1;
  private Random random = new Random();

  protected final SocketFactory socketFactory;           // how to create sockets

  // To avoid indefinite wait, an outstanding RPC will wait in a loop
  // and check every min {defaultWaitTime, rpcTimeout}; until the
  // rpcTimeout is hit.
  final private static String RPC_POLL_INTERVAL_NAME = "rpc.poll.interval";
  final private static int DEFAULT_RPC_POLL_INTERVAL = 50;
  private int defaultWaitTime;

  final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
  final static int DEFAULT_PING_INTERVAL = 60000; // 1 min
  final static int PING_CALL_ID = -1;

  // The maximum size of data read/written to the socket in one shot.
  //
  // sun.nio.ch.Util caches the direct buffers used for socket read/write on a
  // per thread basis, up to 8 buffers per thread. If we read/write large chunks,
  // we will be creating large direct buffers that can hold up memory (multiply
  // this by a factor of #clientThreads).
  public static final int MAX_SOCKET_READ_WRITE_LEN = 131072; // 128k
  /**
   * set the ping interval value in configuration
   *
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  @SuppressWarnings({"UnusedDeclaration"})
  public static void setPingInterval(Configuration conf, int pingInterval) {
    conf.setInt(PING_INTERVAL_NAME, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   *
   * @param conf Configuration
   * @return the ping interval
   */
  static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
  }

  /** A call waiting for a value. */
  private class Call {
    final int id;                                       // call id
    final Writable param;                               // parameter
    HbaseObjectWritable value;                               // value, null if error
    IOException error;                            // exception, null if value
    boolean done;                                 // true when call is done
    protected int version = HBaseServer.CURRENT_VERSION;
    public HBaseRPCOptions options;
    public long startTime;

    protected Call(Writable param) {
      this.param = param;
      synchronized (HBaseClient.this) {
        this.id = counter++;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     *
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }

    /** Set the return value when there is no error.
     * Notify the caller the call is done.
     *
     * @param value return value of the call.
     */
    public synchronized void setValue(HbaseObjectWritable value) {
      this.value = value;
      callComplete();
    }

    public void setVersion(int version) {
      this.version = version; 
    }
     
    public int getVersion() {
      return version;
    }
  }
  
  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends HasThread {
    private ConnectionId remoteId;
    private Socket socket = null;                 // connected socket
    private DataInputStream in;
    private DataOutputStream out;

    // currently active calls
    private final Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private final AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    protected final AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason

    public Connection(ConnectionId remoteId) throws IOException {
      if (remoteId.getAddress().isUnresolved()) {
        throw new UnknownHostException("unknown host: " +
                                       remoteId.getAddress().getHostName());
      }
      this.remoteId = remoteId;
      UserGroupInformation ticket = remoteId.getTicket();
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
        remoteId.getAddress().toString() +
        ((ticket==null)?" from an unknown user": (" from " + ticket.getUserName())));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(System.currentTimeMillis());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    protected synchronized boolean addCall(Call call) throws IOException {
      if (shouldCloseConnection.get()) {
        // If there was something bad. Let not the next thread spend a while
        // figuring it out.
        if (closeException != null)
          throw closeException;
        return false;
      }
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed, send a ping.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() ||
            remoteId.rpcTimeout > 0) {
          throw e;
        }
        sendPing();
      }

      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       *
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }

    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     * @throws java.io.IOException e
     */
    protected synchronized void setupIOstreams(byte version) throws IOException {
      if (socket != null) { // somebody already set up the IOStreams
        return;
      }
      if (shouldCloseConnection.get()) { // somebody already tried and failed.
        throw closeException;
      }

      short ioFailures = 0;
      short timeoutFailures = 0;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+remoteId.getAddress());
        }
        while (true) {
          try {
            this.socket = socketFactory.createSocket();
            this.socket.setTcpNoDelay(tcpNoDelay);
            this.socket.setKeepAlive(tcpKeepAlive);
            NetUtils.connect(this.socket, remoteId.getAddress(),
                connectionTimeOutMillSec, ipTosValue);
            if (remoteId.rpcTimeout > 0) {
              pingInterval = remoteId.rpcTimeout; // overwrite pingInterval
            }
            this.socket.setSoTimeout(pingInterval);
            // discard all the pending send data in the socket buffer and close
            // the connection immediately. The connection is reset instead of
            // being gracefully closed.
            this.socket.setSoLinger(true, 0);
            break;
          } catch (SocketTimeoutException toe) {
            handleConnectionFailure(timeoutFailures++, maxConnectRetries, toe);
          } catch (IOException ie) {
            handleConnectionFailure(ioFailures++, maxConnectRetries, ie);
          }
        }
        this.in = new DataInputStream(new BufferedInputStream
            (new PingInputStream(NetUtils.getInputStream(socket))));
        this.out = new DataOutputStream
            (new BufferedOutputStream(NetUtils.getOutputStream(socket)));
        writeHeader(version);

        // update last activity time
        touch();

        // start the receiver thread after the socket connection has been set up
        start();
      } catch (IOException e) {
        markClosed(e);
        close();

        throw e;
      } catch (Throwable e) {
        IOException ioe = new IOException("Unexpected connection error", e);
        markClosed(ioe);
        close();

        throw ioe;
      }
    }

    /* Handle connection failures
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff N seconds and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionFailure(
        int curRetries, int maxRetries, IOException ioe) throws IOException {
      // close the current connection
      if (socket != null) { // could be null if the socket creation failed
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Not able to close a socket", e);
        }
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        IOException rewrittenException;
        // The NetUtils layer throw a SocketTimeoutException. That behavior is
        // incorrect but too late to change that. Instead rewrite the exception
        // here to ConnectException. The higher layers can use the information
        // that the exception occurred before even sending anything to the
        // peer.
        rewrittenException = new ConnectException();
        rewrittenException.initCause(ioe);
        throw rewrittenException;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(failureSleep);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException();
      }

      LOG.info("Retrying connect to server: " + remoteId.getAddress() +
        " after sleeping " + failureSleep + "ms. Already tried " + curRetries +
        " time(s).");
    }

    /* Write the header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader(byte version) throws IOException {
      out.write(HBaseServer.HEADER.array());
      out.write(version);
      //When there are more fields we can have ConnectionHeader Writable.
      DataOutputBuffer buf = new DataOutputBuffer();
      ObjectWritable.writeObject(buf, remoteId.getTicket(),
                                 UserGroupInformation.class, conf);
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
    }

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed,
     * or the client is marked as not running.
     *
     * Return true if it is time to read a response; false otherwise.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException ignored) {}
        }
      }

      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return remoteId.getAddress();
    }

    /* Send a ping to the server if the time elapsed
     * since last I/O activity is equal to or greater than the ping interval
     */
    protected synchronized void sendPing() throws IOException {
      long curTime = System.currentTimeMillis();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        //noinspection SynchronizeOnNonFinalField
        synchronized (this.out) {
          out.writeInt(PING_CALL_ID);
          out.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections "
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveResponse();
        }
      } catch (Throwable t) {
        LOG.warn("Unexpected exception receiving call responses", t);
        markClosed(new IOException("Unexpected exception receiving call responses", t));
      }

      close();

      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /* Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    protected void sendParam(Call call) {
      if (shouldCloseConnection.get()) {
        return;
      }
      DataOutputStream uncompressedOS = null;
      DataOutputStream outOS = null;
      Compressor compressor = null;
      try {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " sending #" + call.id);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        uncompressedOS = new DataOutputStream(baos);
        outOS = uncompressedOS;
        try {
          // 1. write the call id uncompressed
          uncompressedOS.writeInt(call.id);
          // 2. write RPC options uncompressed
          if (call.version >= HBaseServer.VERSION_RPCOPTIONS) {
            call.options.write(outOS);
          }
          // preserve backwards compatibility
          if (call.options.getTxCompression() != Compression.Algorithm.NONE) {
            // 3. setup the compressor
            compressor = call.options.getTxCompression().getCompressor();
            OutputStream compressedOutputStream =
                call.options.getTxCompression().createCompressionStream(
                    uncompressedOS, compressor, 0);
            outOS = new DataOutputStream(compressedOutputStream);
          }
          // 4. write the output params with the correct compression type
          call.param.write(outOS);
          outOS.flush();
          baos.flush();
          call.startTime = System.currentTimeMillis();
        } catch (IOException e) {
          LOG.error("Failed to prepare request in in-mem buffers!", e);
          markClosed(e);
        }
        byte[] data = baos.toByteArray();
        int dataLength = data.length;
        try {
          synchronized (this.out) {
            out.writeInt(dataLength);      //first put the data length
            writeToSocket(out, data, 0, dataLength);
            out.flush();
          }
        } catch (IOException e) {
          // It is not easy to get an exception here.
          // The read is what always fails. Write gets accepted into
          // the socket buffer. If the connection is already dead, even
          // then read gets called first and fails first.
          IOException rewrittenException =
              new SyncFailedException("Failed to write to peer");
          rewrittenException.initCause(e);
          markClosed(rewrittenException);
        }
      } finally {
        //the buffer is just an in-memory buffer, but it is still polite to
        // close early
        if (outOS != uncompressedOS) {
          IOUtils.closeStream(outOS);
        }
        IOUtils.closeStream(uncompressedOS);
        if (compressor != null) {
          call.options.getTxCompression().returnCompressor(compressor);
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      Compression.Algorithm rpcCompression = null;
      Decompressor decompressor = null;
      try {
        DataInputStream localIn = in;
        
        // 1. Read the call id uncompressed which is an int
        int id = localIn.readInt();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + id);
        Call call = calls.get(id);
        long totalTime = System.currentTimeMillis() - call.startTime;
        // 2. read the error boolean uncompressed
        boolean isError = localIn.readBoolean();
        
        if (call.getVersion() >= HBaseServer.VERSION_RPCOPTIONS) {
          // 3. read the compression type used for the rest of the response
          String compressionAlgoName = localIn.readUTF();
          rpcCompression =  
            Compression.getCompressionAlgorithmByName(compressionAlgoName);
          
          // 4. setup the correct decompressor (if any)
          if (rpcCompression != Compression.Algorithm.NONE) {
            InputStream uncompressedIn;
            if (call.getVersion() >= HBaseServer.VERSION_RPC_COMPRESSION_DATA_LENGTH) {
              int size = in.readInt();
              uncompressedIn = new BoundedInputStream(in, size);
            } else {
              uncompressedIn = in;
            }
            decompressor = rpcCompression.getDecompressor();
            InputStream is = rpcCompression.createDecompressionStream(
                  uncompressedIn, decompressor, 0);
            localIn = new DataInputStream(is);
          }
        }
        // 5. read the rest of the value
        if (isError) {
          //noinspection ThrowableInstanceNeverThrown
          call.setException(new RemoteException( WritableUtils.readString(localIn),
              WritableUtils.readString(localIn)));
          calls.remove(id);
        } else {
          HbaseObjectWritable value = createNewHbaseWritable();
          value.readFields(localIn);                 // read value
          if (call.getVersion() >= HBaseServer.VERSION_RPCOPTIONS) {
            boolean hasProfiling = localIn.readBoolean();
            if (hasProfiling) {
              if (call.options.profilingResult == null) {
                call.options.profilingResult = new ProfilingData();
                call.options.profilingResult.readFields(localIn);
              } else {
                ProfilingData pData = new ProfilingData();
                pData.readFields(localIn);
                call.options.profilingResult.merge(pData);
              }
              Long serverTimeObj = call.options.profilingResult.getLong(
                  ProfilingData.TOTAL_SERVER_TIME_MS);
              if (serverTimeObj != null) {
                call.options.profilingResult.addLong(
                    ProfilingData.CLIENT_NETWORK_LATENCY_MS, 
                    totalTime - serverTimeObj);
              }
            }
          }
          call.setValue(value);
          calls.remove(id);
        }
      } catch (IOException e) {
        markClosed(e);
      } catch (Throwable te) {
        markClosed((IOException)new IOException().initCause(te));
      } finally {
        if (decompressor != null) {
          rpcCompression.returnDecompressor(decompressor);
        }
      }
    }
    
    private HbaseObjectWritable createNewHbaseWritable() {
      HbaseObjectWritable ret = new HbaseObjectWritable();
      ret.setConf(conf);
      return ret;
    }

    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }

    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      connections.remove(remoteId, this);

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + remoteId.address + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }

    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue();
        c.setException(closeException); // local exception
        itor.remove();
      }
    }
  }

  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private final ParallelResults results;
    protected final int index;

    public ParallelCall(Writable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    @Override
    protected void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    protected final HbaseObjectWritable[] values;
    protected int size;
    protected int count;

    public ParallelResults(int size) {
      this.values = new HbaseObjectWritable[size];
      this.size = size;
    }

    /*
     * Collect a result.
     */
    synchronized void callComplete(ParallelCall call) {
      // FindBugs IS2_INCONSISTENT_SYNC
      values[call.index] = call.value;            // store the value
      count++;                                    // count it
      if (count == size)                          // if all values are in
        notify();                                 // then notify waiting caller
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link Writable}
   * class.
   * @param valueClass value class
   * @param conf configuration
   * @param factory socket factory
   */
  public HBaseClient(Configuration conf, SocketFactory factory) {
    this.maxIdleTime =
      conf.getInt("hbase.ipc.client.connection.maxidletime", 10000); //10s
    this.maxConnectRetries =
        conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.failureSleep = conf.getInt(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", false);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.ipTosValue = conf.getInt("hbase.ipc.client.tos.value", NetUtils.NOT_SET_IP_TOS);
    this.pingInterval = getPingInterval(conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The ping interval is" + this.pingInterval + "ms.");
    }
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeOutMillSec =
      conf.getInt("hbase.client.connection.timeout.millsec", 5000);
    this.defaultWaitTime = conf.getInt(RPC_POLL_INTERVAL_NAME, DEFAULT_RPC_POLL_INTERVAL);
    this.numConnectionsPerServer = conf.getInt(NUM_CONNECTIONS_PER_SERVER, 
        DEFAULT_NUM_CONNECTIONS_PER_SERVER);
    LOG.debug("Created a new HBaseClient with " + numConnectionsPerServer + 
        " connections per remote server.");
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass value class
   * @param conf configuration
   */
  public HBaseClient(Configuration conf) {
    this(conf, NetUtils.getDefaultSocketFactory(conf));
  }

  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }

    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }

    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
    }
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @param param writable parameter
   * @param address network address
   * @return Writable
   * @throws IOException e
   */
  public HbaseObjectWritable call(Writable param, InetSocketAddress address, 
      HBaseRPCOptions options) throws IOException {
      return call(param, address, null, 0, options);
  }

  public HbaseObjectWritable call(Writable param, InetSocketAddress addr,
                       UserGroupInformation ticket, int rpcTimeout,
                       HBaseRPCOptions options)
                       throws IOException {
    Call call = new Call(param);
    call.options = options;
    Connection connection = getConnection(addr, ticket, rpcTimeout, call);
    connection.sendParam(call);                 // send the parameter
    boolean interrupted = false;
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    long waitPeriod = rpcTimeout > 0 ? rpcTimeout : defaultWaitTime;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait(waitPeriod);                           // wait for the result
        } catch (InterruptedException ignored) {
          // save the fact that we were interrupted
          interrupted = true;
        }

        if (rpcTimeout > 0 &&
            !call.done
            && EnvironmentEdgeManager.currentTimeMillis() > startTime + rpcTimeout) {
          String msg = "Waited for " + rpcTimeout + " Call " + call.toString() + " still not complete";
          LOG.warn(msg);
          call.setException(new InterruptedIOException(msg));
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        }
        // local exception
        throw wrapException(addr, call.error);
      }
      return call.value;
    }
  }

  /**
   * Take an IOException and the address we were trying to connect to
   * and return an IOException with the input exception as the cause.
   * The new exception provides the stack trace of the place where 
   * the exception is thrown and some extra diagnostics information.
   * 
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  private IOException wrapException(InetSocketAddress addr,
                                         IOException exception) {
    try {
      Class<? extends IOException> c = exception.getClass();
      Constructor<? extends IOException> ctor = c.getConstructor(String.class);
      IOException ioe = ctor.newInstance("RPC call to " + addr +
          " failed on connection exception: " + exception);
      ioe.initCause(exception);
      return ioe;
    } catch (SecurityException e) {
    } catch (NoSuchMethodException e) {
    } catch (IllegalArgumentException e) {
    } catch (InstantiationException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    }
    LOG.warn("Failed to create ioexception instance. RPC call to " + addr +
          " failed on connection exception: " + exception);
    return exception;
  }

  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.
   * @param params writable parameters
   * @param addresses socket addresses
   * @return  Writable[]
   * @throws IOException e
   */
  public HbaseObjectWritable[] call(Writable[] params, InetSocketAddress[] addresses,
      HBaseRPCOptions options)
    throws IOException {
    if (addresses.length == 0) return new HbaseObjectWritable[0];

    ParallelResults results = new ParallelResults(params.length);
    // TODO this synchronization block doesnt make any sense, we should possibly fix it
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        call.options = options;
        try {
          Connection connection = getConnection(addresses[i], null, 0, call);
          connection.sendParam(call);             // send each parameter
        } catch (IOException e) {
          // log errors
          LOG.info("Calling "+addresses[i]+" caught: " +
                   e.getMessage(),e);
          results.size--;                         //  wait for one fewer result
        }
      }
      while (results.count != results.size) {
        try {
          results.wait();                    // wait for all results
        } catch (InterruptedException ignored) {}
      }

      return results.values;
    }
  }

  /* Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  private Connection getConnection(InetSocketAddress addr,
                                   UserGroupInformation ticket,
                                   int rpcTimeout,
                                   Call call)
                                   throws IOException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    // RPC compression is only supported from version 4, so make backward compatible
    byte version = HBaseServer.CURRENT_VERSION;
    if (call.options.getTxCompression() == Compression.Algorithm.NONE
        && call.options.getRxCompression() == Compression.Algorithm.NONE
        && !call.options.getRequestProfiling ()
        && call.options.getTag () == null) {
      version = HBaseServer.VERSION_3;
    }
    call.setVersion(version);
    Connection connection;
    // if multiple connections are enabled per remote, get a random one
    int connectionNum = (numConnectionsPerServer > 1)?
        random.nextInt(numConnectionsPerServer):0;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, ticket, rpcTimeout, 
        call.getVersion(), connectionNum);
    do {
      connection = connections.get(remoteId);
      if (connection == null) {
        // Do not worry about creating a new Connection object if
        // the hash map was already updated. The unused connection
        // will be automatically closed after a 10 sec timeout (maxIdleTime).
        connections.putIfAbsent(remoteId, new Connection(remoteId));
        connection = connections.get(remoteId);
      }
    } while (!connection.addCall(call));

    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams(version);
    return connection;
  }

  /**
   * Write data to the Socket. If data is long, it is split up and
   * written up in parts.
   *
   * sun.nio.ch.Util caches the direct buffers used for socket read/write on a
   * per thread basis, up to 8 buffers per thread. If we write large chunks,
   * we will be creating large direct buffers that can hold up memory (multiply
   * this by a factor of #clientThreads).
   *
   * We avoid this problem by writing small bufffers each time.
   *
   */
  public static void writeToSocket(DataOutput out, byte[] data, int offset, int dataLength)
      throws IOException {
    int totalWritten = 0;
    do {
      int toWrite = (int) ((dataLength - totalWritten < MAX_SOCKET_READ_WRITE_LEN) ?
                         dataLength - totalWritten : MAX_SOCKET_READ_WRITE_LEN);
      out.write(data, offset + totalWritten, toWrite);//write the data
      totalWritten += toWrite;
    } while (totalWritten < dataLength);
  }

  /**
   * Read data from the Socket. If length is too long, it is split up and
   * read it in parts.
   *
   * sun.nio.ch.Util caches the direct buffers used for socket read/write on a
   * per thread basis, up to 8 buffers per thread. If we write large chunks,
   * we will be creating large direct buffers that can hold up memory (multiply
   * this by a factor of #clientThreads).
   *
   * We avoid this problem by reading small bufffers each time.
   *
   */
  public static void readFromSocket(DataInput in, byte[] data, int offset, int dataLength)
     throws IOException {
    int totalRead = 0;
    do {
      int toRead = (int)
          ((dataLength - totalRead < HBaseClient.MAX_SOCKET_READ_WRITE_LEN) ?
             dataLength - totalRead : HBaseClient.MAX_SOCKET_READ_WRITE_LEN);
      in.readFully(data, offset + totalRead, toRead);
      totalRead += toRead;
    } while (totalRead < dataLength);
  }

  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by 
   * <remoteAddress, ticket, RPC version>
   */
  private static class ConnectionId {
    final InetSocketAddress address;
    final UserGroupInformation ticket;
    final private int rpcTimeout;
    final private int version;
    final private int connectionNum;

    ConnectionId(InetSocketAddress address, UserGroupInformation ticket,
        int rpcTimeout, int version, int connectionNum) {
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.version = version;
      this.connectionNum = connectionNum;
    }

    InetSocketAddress getAddress() {
      return address;
    }
    UserGroupInformation getTicket() {
      return ticket;
    }

    @Override
    public boolean equals(Object obj) {
     if (obj instanceof ConnectionId) {
       ConnectionId id = (ConnectionId) obj;
       return address.equals(id.address) && ticket == id.ticket &&
       rpcTimeout == id.rpcTimeout && version == id.version && 
       connectionNum == id.connectionNum;
       //Note : ticket is a ref comparision.
     }
     return false;
    }

    @Override
    public int hashCode() {
      return address.hashCode() ^ System.identityHashCode(ticket) ^ 
          rpcTimeout ^ version ^ connectionNum;
    }
  }
}
