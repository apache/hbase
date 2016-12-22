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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.TraceInfo;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The RPC server with native java NIO implementation deriving from Hadoop to
 * host protobuf described Services. It's the original one before HBASE-17262,
 * and the default RPC server for now.
 *
 * An RpcServer instance has a Listener that hosts the socket.  Listener has fixed number
 * of Readers in an ExecutorPool, 10 by default.  The Listener does an accept and then
 * round robin a Reader is chosen to do the read.  The reader is registered on Selector.  Read does
 * total read off the channel and the parse from which it makes a Call.  The call is wrapped in a
 * CallRunner and passed to the scheduler to be run.  Reader goes back to see if more to be done
 * and loops till done.
 *
 * <p>Scheduler can be variously implemented but default simple scheduler has handlers to which it
 * has given the queues into which calls (i.e. CallRunner instances) are inserted.  Handlers run
 * taking from the queue.  They run the CallRunner#run method on each item gotten from queue
 * and keep taking while the server is up.
 *
 * CallRunner#run executes the call.  When done, asks the included Call to put itself on new
 * queue for Responder to pull from and return result to client.
 *
 * @see BlockingRpcClient
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleRpcServer extends RpcServer {

  protected int port;                             // port we listen on
  protected InetSocketAddress address;            // inet address we listen on
  private int readThreads;                        // number of read threads

  protected int socketSendBufferSize;
  protected final long purgeTimeout;    // in milliseconds

  // maintains the set of client connections and handles idle timeouts
  private ConnectionManager connectionManager;
  private Listener listener = null;
  protected Responder responder = null;

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  @InterfaceStability.Evolving
  public class Call extends RpcServer.Call {

    protected Responder responder;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
        justification="Can't figure why this complaint is happening... see below")
    Call(int id, final BlockingService service, final MethodDescriptor md,
        RequestHeader header, Message param, CellScanner cellScanner,
        Connection connection, Responder responder, long size, TraceInfo tinfo,
        final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
      super(id, service, md, header, param, cellScanner, connection, size,
          tinfo, remoteAddress, timeout, reqCleanup);
      this.responder = responder;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is now safe to
     * cleanup.
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
        justification="Presume the lock on processing request held by caller is protection enough")
    void done() {
      super.done();
      this.getConnection().decRpcCount(); // Say that we're done with this call.
    }

    @Override
    public long disconnectSince() {
      if (!getConnection().isConnectionOpen()) {
        return System.currentTimeMillis() - timestamp;
      } else {
        return -1L;
      }
    }

    public synchronized void sendResponseIfReady() throws IOException {
      // set param null to reduce memory pressure
      this.param = null;
      this.responder.doRespond(this);
    }

    Connection getConnection() {
      return (Connection) this.connection;
    }

  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private final int readerPendingConnectionQueueLength;

    private ExecutorService readPool;

    public Listener(final String name) throws IOException {
      super(name);
      // The backlog of requests that we will have the serversocket carry.
      int backlogLength = conf.getInt("hbase.ipc.server.listen.queue.size", 128);
      readerPendingConnectionQueueLength =
          conf.getInt("hbase.ipc.server.read.connection-queue.size", 100);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the binding addrees (can be different from the default interface)
      bind(acceptChannel.socket(), bindAddress, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      address = (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
      // create a selector;
      selector = Selector.open();

      readers = new Reader[readThreads];
      // Why this executor thing? Why not like hadoop just start up all the threads? I suppose it
      // has an advantage in that it is easy to shutdown the pool.
      readPool = Executors.newFixedThreadPool(readThreads,
        new ThreadFactoryBuilder().setNameFormat(
          "RpcServer.reader=%d,bindAddress=" + bindAddress.getHostName() +
          ",port=" + port).setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
      for (int i = 0; i < readThreads; ++i) {
        Reader reader = new Reader();
        readers[i] = reader;
        readPool.execute(reader);
      }
      LOG.info(getName() + ": started " + readThreads + " reader(s) listening on port=" + port);

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("RpcServer.listener,port=" + port);
      this.setDaemon(true);
    }


    private class Reader implements Runnable {
      final private LinkedBlockingQueue<Connection> pendingConnections;
      private final Selector readSelector;

      Reader() throws IOException {
        this.pendingConnections =
          new LinkedBlockingQueue<Connection>(readerPendingConnectionQueueLength);
        this.readSelector = Selector.open();
      }

      @Override
      public void run() {
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error(getName() + ": error closing read selector in " + getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          try {
            // Consume as many connections as currently queued to avoid
            // unbridled acceptance of connections that starves the select
            int size = pendingConnections.size();
            for (int i=size; i>0; i--) {
              Connection conn = pendingConnections.take();
              conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
            }
            readSelector.select();
            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
              key = null;
            }
          } catch (InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            }
            return;
          } catch (IOException ex) {
            LOG.info(getName() + ": IOException in Reader", ex);
          }
        }
      }

      /**
       * Updating the readSelector while it's being used is not thread-safe,
       * so the connection must be queued.  The reader will drain the queue
       * and update its readSelector before performing the next select
       */
      public void addConnection(Connection conn) throws IOException {
        pendingConnections.add(conn);
        readSelector.wakeup();
      }
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
      justification="selector access is not synchronized; seems fine but concerned changing " +
        "it will have per impact")
    public void run() {
      LOG.info(getName() + ": starting");
      connectionManager.startIdleScan();
      while (running) {
        SelectionKey key = null;
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException ignored) {
              if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              closeCurrentConnection(key, e);
              connectionManager.closeIdle(true);
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            closeCurrentConnection(key, e);
            connectionManager.closeIdle(true);
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
            }
          }
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
      }
      LOG.info(getName() + ": stopping");
      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) {
          if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
        }

        selector= null;
        acceptChannel= null;

        // close all connections
        connectionManager.stopIdleScan();
        connectionManager.closeAll();
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          closeConnection(c);
          key.attach(null);
        }
      }
    }

    InetSocketAddress getAddress() {
      return address;
    }

    void doAccept(SelectionKey key) throws InterruptedException, IOException, OutOfMemoryError {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel channel;
      while ((channel = server.accept()) != null) {
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(tcpKeepAlive);
        Reader reader = getReader();
        Connection c = connectionManager.register(channel);
        // If the connectionManager can't take it, close the connection.
        if (c == null) {
          if (channel.isOpen()) {
            IOUtils.cleanup(null, channel);
          }
          continue;
        }
        key.attach(c);  // so closeCurrentConnection can get the object
        reader.addConnection(c);
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count;
      Connection c = (Connection) key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        LOG.info(Thread.currentThread().getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": Caught exception while reading:", e);
        }
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        closeConnection(c);
        c = null;
      } else {
        c.setLastContact(System.currentTimeMillis());
      }
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
          LOG.info(getName() + ": exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  protected class Responder extends Thread {
    private final Selector writeSelector;
    private final Set<Connection> writingCons =
        Collections.newSetFromMap(new ConcurrentHashMap<Connection, Boolean>());

    Responder() throws IOException {
      this.setName("RpcServer.responder");
      this.setDaemon(true);
      this.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
      writeSelector = Selector.open(); // create a selector
    }

    @Override
    public void run() {
      LOG.debug(getName() + ": starting");
      try {
        doRunLoop();
      } finally {
        LOG.info(getName() + ": stopping");
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error(getName() + ": couldn't close write selector", ioe);
        }
      }
    }

    /**
     * Take the list of the connections that want to write, and register them
     * in the selector.
     */
    private void registerWrites() {
      Iterator<Connection> it = writingCons.iterator();
      while (it.hasNext()) {
        Connection c = it.next();
        it.remove();
        SelectionKey sk = c.channel.keyFor(writeSelector);
        try {
          if (sk == null) {
            try {
              c.channel.register(writeSelector, SelectionKey.OP_WRITE, c);
            } catch (ClosedChannelException e) {
              // ignore: the client went away.
              if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
            }
          } else {
            sk.interestOps(SelectionKey.OP_WRITE);
          }
        } catch (CancelledKeyException e) {
          // ignore: the client went away.
          if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
        }
      }
    }

    /**
     * Add a connection to the list that want to write,
     */
    public void registerForWrite(Connection c) {
      if (writingCons.add(c)) {
        writeSelector.wakeup();
      }
    }

    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.
      while (running) {
        try {
          registerWrites();
          int keyCt = writeSelector.select(purgeTimeout);
          if (keyCt == 0) {
            continue;
          }

          Set<SelectionKey> keys = writeSelector.selectedKeys();
          Iterator<SelectionKey> iter = keys.iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.debug(getName() + ": asyncWrite", e);
            }
          }

          lastPurgeTime = purge(lastPurgeTime);

        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
              return;
            }
          }
        } catch (Exception e) {
          LOG.warn(getName() + ": exception in Responder " +
              StringUtils.stringifyException(e), e);
        }
      }
      LOG.info(getName() + ": stopped");
    }

    /**
     * If there were some calls that have not been sent out for a
     * long time, we close the connection.
     * @return the time of the purge.
     */
    private long purge(long lastPurgeTime) {
      long now = System.currentTimeMillis();
      if (now < lastPurgeTime + purgeTimeout) {
        return lastPurgeTime;
      }

      ArrayList<Connection> conWithOldCalls = new ArrayList<Connection>();
      // get the list of channels from list of keys.
      synchronized (writeSelector.keys()) {
        for (SelectionKey key : writeSelector.keys()) {
          Connection connection = (Connection) key.attachment();
          if (connection == null) {
            throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
          }
          Call call = connection.responseQueue.peekFirst();
          if (call != null && now > call.timestamp + purgeTimeout) {
            conWithOldCalls.add(call.getConnection());
          }
        }
      }

      // Seems safer to close the connection outside of the synchronized loop...
      for (Connection connection : conWithOldCalls) {
        closeConnection(connection);
      }

      return now;
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Connection connection = (Connection) key.attachment();
      if (connection == null) {
        throw new IOException("doAsyncWrite: no connection");
      }
      if (key.channel() != connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      if (processAllResponses(connection)) {
        try {
          // We wrote everything, so we don't need to be told when the socket is ready for
          //  write anymore.
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

    /**
     * Process the response for this call. You need to have the lock on
     * {@link org.apache.hadoop.hbase.ipc.SimpleRpcServer.Connection#responseWriteLock}
     *
     * @param call the call
     * @return true if we proceed the call fully, false otherwise.
     * @throws IOException
     */
    private boolean processResponse(final Call call) throws IOException {
      boolean error = true;
      try {
        // Send as much data as we can in the non-blocking fashion
        long numBytes = channelWrite(call.getConnection().channel,
            call.response);
        if (numBytes < 0) {
          throw new HBaseIOException("Error writing on the socket " +
            "for the call:" + call.toShortString());
        }
        error = false;
      } finally {
        if (error) {
          LOG.debug(getName() + call.toShortString() + ": output error -- closing");
          // We will be closing this connection itself. Mark this call as done so that all the
          // buffer(s) it got from pool can get released
          call.done();
          closeConnection(call.getConnection());
        }
      }

      if (!call.response.hasRemaining()) {
        call.done();
        return true;
      } else {
        return false; // Socket can't take more, we will have to come back.
      }
    }

    /**
     * Process all the responses for this connection
     *
     * @return true if all the calls were processed or that someone else is doing it.
     * false if there * is still some work to do. In this case, we expect the caller to
     * delay us.
     * @throws IOException
     */
    private boolean processAllResponses(final Connection connection) throws IOException {
      // We want only one writer on the channel for a connection at a time.
      connection.responseWriteLock.lock();
      try {
        for (int i = 0; i < 20; i++) {
          // protection if some handlers manage to need all the responder
          Call call = connection.responseQueue.pollFirst();
          if (call == null) {
            return true;
          }
          if (!processResponse(call)) {
            connection.responseQueue.addFirst(call);
            return false;
          }
        }
      } finally {
        connection.responseWriteLock.unlock();
      }

      return connection.responseQueue.isEmpty();
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      boolean added = false;

      // If there is already a write in progress, we don't wait. This allows to free the handlers
      //  immediately for other tasks.
      if (call.getConnection().responseQueue.isEmpty()
          && call.getConnection().responseWriteLock.tryLock()) {
        try {
          if (call.getConnection().responseQueue.isEmpty()) {
            // If we're alone, we can try to do a direct call to the socket. It's
            //  an optimisation to save on context switches and data transfer between cores..
            if (processResponse(call)) {
              return; // we're done.
            }
            // Too big to fit, putting ahead.
            call.getConnection().responseQueue.addFirst(call);
            added = true; // We will register to the selector later, outside of the lock.
          }
        } finally {
          call.getConnection().responseWriteLock.unlock();
        }
      }

      if (!added) {
        call.getConnection().responseQueue.addLast(call);
      }
      call.responder.registerForWrite(call.getConnection());

      // set the serve time when the response has to be sent later
      call.timestamp = System.currentTimeMillis();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  public class Connection extends RpcServer.Connection {

    protected SocketChannel channel;
    private ByteBuff data;
    private ByteBuffer dataLengthBuffer;
    protected final ConcurrentLinkedDeque<Call> responseQueue = new ConcurrentLinkedDeque<Call>();
    private final Lock responseWriteLock = new ReentrantLock();
    private LongAdder rpcCount = new LongAdder(); // number of outstanding rpcs
    private long lastContact;
    protected Socket socket;

    private ByteBuffer unwrappedData;
    // When is this set?  FindBugs wants to know!  Says NP
    private ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);

    private final Call authFailedCall = new Call(AUTHORIZATION_FAILED_CALLID, null, null, null,
        null, null, this, null, 0, null, null, 0, null);

    private final Call saslCall = new Call(SASL_CALLID, null, null, null, null, null, this, null,
        0, null, null, 0, null);

    private final Call setConnectionHeaderResponseCall = new Call(CONNECTION_HEADER_RESPONSE_CALLID,
        null, null, null, null, null, this, null, 0, null, null, 0, null);

    public Connection(SocketChannel channel, long lastContact) {
      super();
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount.sum() == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount.decrement();
    }

    /* Increment the outstanding RPC count */
    protected void incRpcCount() {
      rpcCount.increment();
    }

    private void saslReadAndProcess(ByteBuff saslToken) throws IOException,
        InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled())
          LOG.trace("Have read input token of size " + saslToken.limit()
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.hasArray() ? saslToken.array() : saslToken.toBytes();
          byte [] plaintextData;
          if (useCryptoAesWrap) {
            // unwrap with CryptoAES
            plaintextData = cryptoAES.unwrap(b, 0, b.length);
          } else {
            plaintextData = saslServer.unwrap(b, 0, b.length);
          }
          processUnwrappedData(plaintextData);
        }
      } else {
        byte[] replyToken;
        try {
          if (saslServer == null) {
            switch (authMethod) {
            case DIGEST:
              if (secretManager == null) {
                throw new AccessDeniedException(
                    "Server is not configured to do DIGEST authentication.");
              }
              saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
                  .getMechanismName(), null, SaslUtil.SASL_DEFAULT_REALM,
                  HBaseSaslRpcServer.getSaslProps(), new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation.getCurrentUser();
              String fullName = current.getUserName();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal name is " + fullName);
              }
              final String names[] = SaslUtil.splitKerberosName(fullName);
              if (names.length != 3) {
                throw new AccessDeniedException(
                    "Kerberos principal name does NOT have the expected "
                        + "hostname part: " + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
                      .getMechanismName(), names[0], names[1],
                      HBaseSaslRpcServer.getSaslProps(), new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null)
              throw new AccessDeniedException(
                  "Unable to find SASL server implementation for "
                      + authMethod.getMechanismName());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created SASL server with mechanism = " + authMethod.getMechanismName());
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.limit()
                + " for processing by saslServer.evaluateResponse()");
          }
          replyToken = saslServer
              .evaluateResponse(saslToken.hasArray() ? saslToken.array() : saslToken.toBytes());
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
            sendToClient.getLocalizedMessage());
          metrics.authenticationFailure();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
          throw e;
        }
        if (replyToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + replyToken.length
                + " from saslServer.");
          }
          doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
              null);
        }
        if (saslServer.isComplete()) {
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          ugi = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: "
              + ugi + ". Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
          saslContextEstablished = true;
        }
      }
    }

    /**
     * No protobuf encoding of raw sasl messages
     */
    private void doRawSaslReply(SaslStatus status, Writable rv,
        String errorClass, String error) throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity is 256.
        saslResponse = new ByteBufferOutputStream(256);
        out = new DataOutputStream(saslResponse);
        out.writeInt(status.state); // write status
        if (status == SaslStatus.SUCCESS) {
          rv.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
        saslCall.setSaslTokenResponse(saslResponse.getByteBuffer());
        saslCall.responder = responder;
        saslCall.sendResponseIfReady();
      } finally {
        if (saslResponse != null) {
          saslResponse.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }

    /**
     * Send the response for connection header
     */
    private void doConnectionHeaderResponse(byte[] wrappedCipherMetaData) throws IOException {
      ByteBufferOutputStream response = null;
      DataOutputStream out = null;
      try {
        response = new ByteBufferOutputStream(wrappedCipherMetaData.length + 4);
        out = new DataOutputStream(response);
        out.writeInt(wrappedCipherMetaData.length);
        out.write(wrappedCipherMetaData);

        setConnectionHeaderResponseCall.setConnectionHeaderResponse(response.getByteBuffer());
        setConnectionHeaderResponseCall.responder = responder;
        setConnectionHeaderResponseCall.sendResponseIfReady();
      } finally {
        if (out != null) {
          out.close();
        }
        if (response != null) {
          response.close();
        }
      }
    }

    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
          saslServer = null;
        } catch (SaslException ignored) {
          // Ignored. This is being disposed of anyway.
        }
      }
    }

    private int readPreamble() throws IOException {
      int count;
      // Check for 'HBas' magic.
      this.dataLengthBuffer.flip();
      if (!Arrays.equals(HConstants.RPC_HEADER, dataLengthBuffer.array())) {
        return doBadPreambleHandling("Expected HEADER=" +
            Bytes.toStringBinary(HConstants.RPC_HEADER) +
            " but received HEADER=" + Bytes.toStringBinary(dataLengthBuffer.array()) +
            " from " + toString());
      }
      // Now read the next two bytes, the version and the auth to use.
      ByteBuffer versionAndAuthBytes = ByteBuffer.allocate(2);
      count = channelRead(channel, versionAndAuthBytes);
      if (count < 0 || versionAndAuthBytes.remaining() > 0) {
        return count;
      }
      int version = versionAndAuthBytes.get(0);
      byte authbyte = versionAndAuthBytes.get(1);
      this.authMethod = AuthMethod.valueOf(authbyte);
      if (version != CURRENT_VERSION) {
        String msg = getFatalConnectionString(version, authbyte);
        return doBadPreambleHandling(msg, new WrongVersionException(msg));
      }
      if (authMethod == null) {
        String msg = getFatalConnectionString(version, authbyte);
        return doBadPreambleHandling(msg, new BadAuthException(msg));
      }
      if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
        if (allowFallbackToSimpleAuth) {
          metrics.authenticationFallback();
          authenticatedWithFallback = true;
        } else {
          AccessDeniedException ae = new AccessDeniedException("Authentication is required");
          setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
          responder.doRespond(authFailedCall);
          throw ae;
        }
      }
      if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
        doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(
            SaslUtil.SWITCH_TO_SIMPLE_AUTH), null, null);
        authMethod = AuthMethod.SIMPLE;
        // client has already sent the initial Sasl message and we
        // should ignore it. Both client and server should fall back
        // to simple auth from now on.
        skipInitialSaslHandshake = true;
      }
      if (authMethod != AuthMethod.SIMPLE) {
        useSasl = true;
      }

      dataLengthBuffer.clear();
      connectionPreambleRead = true;
      return count;
    }

    private int read4Bytes() throws IOException {
      if (this.dataLengthBuffer.remaining() > 0) {
        return channelRead(channel, this.dataLengthBuffer);
      } else {
        return 0;
      }
    }

    /**
     * Read off the wire. If there is not enough data to read, update the connection state with
     *  what we have and returns.
     * @return Returns -1 if failure (and caller will close connection), else zero or more.
     * @throws IOException
     * @throws InterruptedException
     */
    public int readAndProcess() throws IOException, InterruptedException {
      // Try and read in an int.  If new connection, the int will hold the 'HBas' HEADER.  If it
      // does, read in the rest of the connection preamble, the version and the auth method.
      // Else it will be length of the data to read (or -1 if a ping).  We catch the integer
      // length into the 4-byte this.dataLengthBuffer.
      int count = read4Bytes();
      if (count < 0 || dataLengthBuffer.remaining() > 0) {
        return count;
      }

      // If we have not read the connection setup preamble, look to see if that is on the wire.
      if (!connectionPreambleRead) {
        count = readPreamble();
        if (!connectionPreambleRead) {
          return count;
        }

        count = read4Bytes();
        if (count < 0 || dataLengthBuffer.remaining() > 0) {
          return count;
        }
      }

      // We have read a length and we have read the preamble.  It is either the connection header
      // or it is a request.
      if (data == null) {
        dataLengthBuffer.flip();
        int dataLength = dataLengthBuffer.getInt();
        if (dataLength == RpcClient.PING_CALL_ID) {
          if (!useWrap) { //covers the !useSasl too
            dataLengthBuffer.clear();
            return 0;  //ping message
          }
        }
        if (dataLength < 0) { // A data length of zero is legal.
          throw new DoNotRetryIOException("Unexpected data length "
              + dataLength + "!! from " + getHostAddress());
        }

        if (dataLength > maxRequestSize) {
          String msg = "RPC data length of " + dataLength + " received from "
              + getHostAddress() + " is greater than max allowed "
              + maxRequestSize + ". Set \"" + MAX_REQUEST_SIZE
              + "\" on server to override this limit (not recommended)";
          LOG.warn(msg);

          if (connectionHeaderRead && connectionPreambleRead) {
            incRpcCount();
            // Construct InputStream for the non-blocking SocketChannel
            // We need the InputStream because we want to read only the request header
            // instead of the whole rpc.
            ByteBuffer buf = ByteBuffer.allocate(1);
            InputStream is = new InputStream() {
              @Override
              public int read() throws IOException {
                channelRead(channel, buf);
                buf.flip();
                int x = buf.get();
                buf.flip();
                return x;
              }
            };
            CodedInputStream cis = CodedInputStream.newInstance(is);
            int headerSize = cis.readRawVarint32();
            Message.Builder builder = RequestHeader.newBuilder();
            ProtobufUtil.mergeFrom(builder, cis, headerSize);
            RequestHeader header = (RequestHeader) builder.build();

            // Notify the client about the offending request
            Call reqTooBig = new Call(header.getCallId(), this.service, null, null, null,
                null, this, responder, 0, null, this.addr, 0, null);
            metrics.exception(REQUEST_TOO_BIG_EXCEPTION);
            // Make sure the client recognizes the underlying exception
            // Otherwise, throw a DoNotRetryIOException.
            if (VersionInfoUtil.hasMinimumVersion(connectionHeader.getVersionInfo(),
                RequestTooBigException.MAJOR_VERSION, RequestTooBigException.MINOR_VERSION)) {
              setupResponse(null, reqTooBig, REQUEST_TOO_BIG_EXCEPTION, msg);
            } else {
              setupResponse(null, reqTooBig, new DoNotRetryIOException(), msg);
            }
            // We are going to close the connection, make sure we process the response
            // before that. In rare case when this fails, we still close the connection.
            responseWriteLock.lock();
            responder.processResponse(reqTooBig);
            responseWriteLock.unlock();
          }
          // Close the connection
          return -1;
        }

        // Initialize this.data with a ByteBuff.
        // This call will allocate a ByteBuff to read request into and assign to this.data
        // Also when we use some buffer(s) from pool, it will create a CallCleanup instance also and
        // assign to this.callCleanup
        initByteBuffToReadInto(dataLength);

        // Increment the rpc count. This counter will be decreased when we write
        //  the response.  If we want the connection to be detected as idle properly, we
        //  need to keep the inc / dec correct.
        incRpcCount();
      }

      count = channelDataRead(channel, data);

      if (count >= 0 && data.remaining() == 0) { // count==0 if dataLength == 0
        process();
      }

      return count;
    }

    // It creates the ByteBuff and CallCleanup and assign to Connection instance.
    private void initByteBuffToReadInto(int length) {
      // We create random on heap buffers are read into those when
      // 1. ByteBufferPool is not there.
      // 2. When the size of the req is very small. Using a large sized (64 KB) buffer from pool is
      // waste then. Also if all the reqs are of this size, we will be creating larger sized
      // buffers and pool them permanently. This include Scan/Get request and DDL kind of reqs like
      // RegionOpen.
      // 3. If it is an initial handshake signal or initial connection request. Any way then
      // condition 2 itself will match
      // 4. When SASL use is ON.
      if (reservoir == null || skipInitialSaslHandshake || !connectionHeaderRead || useSasl
          || length < minSizeForReservoirUse) {
        this.data = new SingleByteBuff(ByteBuffer.allocate(length));
      } else {
        Pair<ByteBuff, CallCleanup> pair = RpcServer.allocateByteBuffToReadInto(reservoir,
            minSizeForReservoirUse, length);
        this.data = pair.getFirst();
        this.callCleanup = pair.getSecond();
      }
    }

    protected int channelDataRead(ReadableByteChannel channel, ByteBuff buf) throws IOException {
      int count = buf.read(channel);
      if (count > 0) {
        metrics.receivedBytes(count);
      }
      return count;
    }

    /**
     * Process the data buffer and clean the connection state for the next call.
     */
    private void process() throws IOException, InterruptedException {
      data.rewind();
      try {
        if (skipInitialSaslHandshake) {
          skipInitialSaslHandshake = false;
          return;
        }

        if (useSasl) {
          saslReadAndProcess(data);
        } else {
          processOneRpc(data);
        }

      } finally {
        dataLengthBuffer.clear(); // Clean for the next call
        data = null; // For the GC
        this.callCleanup = null;
      }
    }

    private int doBadPreambleHandling(final String msg) throws IOException {
      return doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private int doBadPreambleHandling(final String msg, final Exception e) throws IOException {
      LOG.warn(msg);
      Call fakeCall = new Call(-1, null, null, null, null, null, this, responder, -1, null, null, 0,
          null);
      setupResponse(null, fakeCall, e, msg);
      responder.doRespond(fakeCall);
      // Returning -1 closes out the connection.
      return -1;
    }

    // Reads the connection header following version
    private void processConnectionHeader(ByteBuff buf) throws IOException {
      if (buf.hasArray()) {
        this.connectionHeader = ConnectionHeader.parseFrom(buf.array());
      } else {
        CodedInputStream cis = UnsafeByteOperations
            .unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
        cis.enableAliasing(true);
        this.connectionHeader = ConnectionHeader.parseFrom(cis);
      }
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) throw new EmptyServiceNameException();
      this.service = getService(services, serviceName);
      if (this.service == null) throw new UnknownServiceException(serviceName);
      setupCellBlockCodecs(this.connectionHeader);
      RPCProtos.ConnectionHeaderResponse.Builder chrBuilder =
          RPCProtos.ConnectionHeaderResponse.newBuilder();
      setupCryptoCipher(this.connectionHeader, chrBuilder);
      responseConnectionHeader(chrBuilder);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        ugi = protocolUser;
        if (ugi != null) {
          ugi.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
        // audit logging for SASL authenticated users happens in saslReadAndProcess()
        if (authenticatedWithFallback) {
          LOG.warn("Allowed fallback to SIMPLE auth for " + ugi
              + " connecting from " + getHostAddress());
        }
        AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
      } else {
        // user is authenticated
        ugi.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + ugi
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = ugi;
            ugi = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        // see if this connection will support RetryImmediatelyException
        retryImmediatelySupported = VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);

        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with version info: "
            + TextFormat.shortDebugString(connectionHeader.getVersionInfo()));
      } else {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with unknown version info");
      }
    }

    private void responseConnectionHeader(RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
        throws FatalConnectionException {
      // Response the connection header if Crypto AES is enabled
      if (!chrBuilder.hasCryptoCipherMeta()) return;
      try {
        byte[] connectionHeaderResBytes = chrBuilder.build().toByteArray();
        // encrypt the Crypto AES cipher meta data with sasl server, and send to client
        byte[] unwrapped = new byte[connectionHeaderResBytes.length + 4];
        Bytes.putBytes(unwrapped, 0, Bytes.toBytes(connectionHeaderResBytes.length), 0, 4);
        Bytes.putBytes(unwrapped, 4, connectionHeaderResBytes, 0, connectionHeaderResBytes.length);

        doConnectionHeaderResponse(saslServer.wrap(unwrapped, 0, unwrapped.length));
      } catch (IOException ex) {
        throw new UnsupportedCryptoException(ex.getMessage(), ex);
      }
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException,
    InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(new SingleByteBuff(unwrappedData));
          unwrappedData = null;
        }
      }
    }

    private void processOneRpc(ByteBuff buf) throws IOException, InterruptedException {
      if (connectionHeaderRead) {
        processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service " +
            connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
        }
        this.user = userProvider.create(this.ugi);
      }
    }

    /**
     * @param buf Has the request header and the request param and optionally encoded data buffer
     * all in this one array.
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processRequest(ByteBuff buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header.  We avoid having pb
      // do its default 4k allocation for CodedInputStream.  We force it to use backing array.
      CodedInputStream cis;
      if (buf.hasArray()) {
        cis = UnsafeByteOperations.unsafeWrap(buf.array(), 0, buf.limit()).newCodedInput();
      } else {
        cis = UnsafeByteOperations
            .unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
      }
      cis.enableAliasing(true);
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) +
          " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the total request.
      if ((totalRequestSize + callQueueSizeInBytes.sum()) > maxQueueSizeInBytes) {
        final Call callTooBig =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null, 0, this.callCleanup);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + server.getServerName() +
                ", is hbase.ipc.server.max.callqueue.size too small?");
        responder.doRespond(callTooBig);
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (md == null) throw new UnsupportedOperationException(header.getMethodName());
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        } else {
          // currently header must have request param, so we directly throw exception here
          String msg = "Invalid request header: " + TextFormat.shortDebugString(header)
              + ", should have param set in it";
          LOG.warn(msg);
          throw new DoNotRetryIOException(msg);
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          ByteBuff dup = buf.duplicate();
          dup.limit(offset + header.getCellBlockMeta().getLength());
          cellScanner = cellBlockBuilder.createCellScannerReusingBuffers(this.codec,
              this.compressionCodec, dup);
        }
      } catch (Throwable t) {
        InetSocketAddress address = getListenerAddress();
        String msg = (address != null ? address : "(channel closed)") +
            " is unable to read call parameter from client " + getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running hadoop version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final Call readParamsFailedCall =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null, 0, this.callCleanup);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t,
          msg + "; " + t.getMessage());
        responder.doRespond(readParamsFailedCall);
        return;
      }

      TraceInfo traceInfo = header.hasTraceInfo()
          ? new TraceInfo(header.getTraceInfo().getTraceId(), header.getTraceInfo().getParentId())
          : null;
      int timeout = 0;
      if (header.hasTimeout() && header.getTimeout() > 0){
        timeout = Math.max(minClientRequestTimeout, header.getTimeout());
      }
      Call call = new Call(id, this.service, md, header, param, cellScanner, this, responder,
          totalRequestSize, traceInfo, this.addr, timeout, this.callCleanup);

      if (!scheduler.dispatch(new CallRunner(SimpleRpcServer.this, call))) {
        callQueueSizeInBytes.add(-1 * call.getSize());

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + server.getServerName() +
                ", too many items queued ?");
        responder.doRespond(call);
      }
    }

    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (ugi != null && ugi.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(ugi, this.getHostAddress(), conf);
        }
        authorize(ugi, connectionHeader, getHostInetAddress());
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        }
        metrics.authorizationFailure();
        setupResponse(authFailedResponse, authFailedCall,
          new AccessDeniedException(ae), ae.getMessage());
        responder.doRespond(authFailedCall);
        return false;
      }
      return true;
    }

    protected synchronized void close() {
      disposeSasl();
      data = null;
      callCleanup = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception ignored) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignored exception", ignored);
        }
      }
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {
        socket.close();
      } catch(Exception ignored) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignored exception", ignored);
        }
      }
    }

    @Override
    public boolean isConnectionOpen() {
      return channel.isOpen();
    }
  }


  /**
   * Constructs a server listening on the named port and address.
   * @param server hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param bindAddress Where to listen
   * @param conf
   * @param scheduler
   */
  public SimpleRpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler)
      throws IOException {
    super(server, name, services, bindAddress, conf, scheduler);
    this.socketSendBufferSize = 0;
    this.readThreads = conf.getInt("hbase.ipc.server.read.threadpool.size", 10);
    this.purgeTimeout = conf.getLong("hbase.ipc.client.call.purge.timeout",
      2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    // Start the listener here and let it bind to the port
    listener = new Listener(name);
    this.port = listener.getAddress().getPort();

    // Create the responder here
    responder = new Responder();
    connectionManager = new ConnectionManager();
    initReconfigurable(conf);

    this.scheduler.init(new RpcSchedulerContext(this));
  }

  /**
   * Subclasses of HBaseServer can override this to provide their own
   * Connection implementations.
   */
  protected Connection getConnection(SocketChannel channel, long time) {
    return new Connection(channel, time);
  }

  /**
   * Setup response for the RPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response, Call call, Throwable t, String error)
  throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  protected void closeConnection(Connection connection) {
    connectionManager.close(connection);
  }

  /** Sets the socket buffer size used for responding to RPCs.
   * @param size send size
   */
  @Override
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public synchronized void start() {
    if (started) return;
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      setSecretManager(authTokenSecretMgr);
      authTokenSecretMgr.start();
    }
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    responder.start();
    listener.start();
    scheduler.start();
    started = true;
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (authTokenSecretMgr != null) {
      authTokenSecretMgr.stop();
      authTokenSecretMgr = null;
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    scheduler.stop();
    notifyAll();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  @Override
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to. May return null if
   * the listener channel is closed.
   * @return the socket (ip+port) on which the RPC server is listening to, or null if this
   * information cannot be determined
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    if (listener == null) {
      return null;
    }
    return listener.getAddress();
  }

  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
      throws IOException {
    return call(service, md, param, cellScanner, receiveTime, status, System.currentTimeMillis(),0);
  }

  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md, Message param,
      CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status, long startTime,
      int timeout)
      throws IOException {
    Call fakeCall = new Call(-1, service, md, null, param, cellScanner, null, null, -1, null, null, timeout,
      null);
    fakeCall.setReceiveTime(receiveTime);
    return call(fakeCall, status);
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 64 * 1024; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @param channel writable byte channel to write to
   * @param bufferChain Chain of buffers to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
   */
  protected long channelWrite(GatheringByteChannel channel, BufferChain bufferChain)
  throws IOException {
    long count =  bufferChain.write(channel, NIO_BUFFER_LIMIT);
    if (count > 0) this.metrics.sentBytes(count);
    return count;
  }

  /**
   * This is a wrapper around {@link java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
   */
  protected int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}
   * and {@link #channelWrite(GatheringByteChannel, BufferChain)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   * @see #channelWrite(GatheringByteChannel, BufferChain)
   */
  protected static int channelIO(ReadableByteChannel readCh,
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

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return connectionManager.size();
  }

  private class ConnectionManager {
    final private AtomicInteger count = new AtomicInteger();
    final private Set<Connection> connections;

    final private Timer idleScanTimer;
    final private int idleScanThreshold;
    final private int idleScanInterval;
    final private int maxIdleTime;
    final private int maxIdleToClose;

    ConnectionManager() {
      this.idleScanTimer = new Timer("RpcServer idle connection scanner for port " + port, true);
      this.idleScanThreshold = conf.getInt("hbase.ipc.client.idlethreshold", 4000);
      this.idleScanInterval =
          conf.getInt("hbase.ipc.client.connection.idle-scan-interval.ms", 10000);
      this.maxIdleTime = 2 * conf.getInt("hbase.ipc.client.connection.maxidletime", 10000);
      this.maxIdleToClose = conf.getInt("hbase.ipc.client.kill.max", 10);
      int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
      int maxConnectionQueueSize =
          handlerCount * conf.getInt("hbase.ipc.server.handler.queue.size", 100);
      // create a set with concurrency -and- a thread-safe iterator, add 2
      // for listener and idle closer threads
      this.connections = Collections.newSetFromMap(
          new ConcurrentHashMap<Connection,Boolean>(
              maxConnectionQueueSize, 0.75f, readThreads+2));
    }

    private boolean add(Connection connection) {
      boolean added = connections.add(connection);
      if (added) {
        count.getAndIncrement();
      }
      return added;
    }

    private boolean remove(Connection connection) {
      boolean removed = connections.remove(connection);
      if (removed) {
        count.getAndDecrement();
      }
      return removed;
    }

    int size() {
      return count.get();
    }

    Connection[] toArray() {
      return connections.toArray(new Connection[0]);
    }

    Connection register(SocketChannel channel) {
      Connection connection = getConnection(channel, System.currentTimeMillis());
      add(connection);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server connection from " + connection +
            "; connections=" + size() +
            ", queued calls size (bytes)=" + callQueueSizeInBytes.sum() +
            ", general queued calls=" + scheduler.getGeneralQueueLength() +
            ", priority queued calls=" + scheduler.getPriorityQueueLength());
      }
      return connection;
    }

    boolean close(Connection connection) {
      boolean exists = remove(connection);
      if (exists) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() +
              ": disconnecting client " + connection +
              ". Number of active connections: "+ size());
        }
        // only close if actually removed to avoid double-closing due
        // to possible races
        connection.close();
      }
      return exists;
    }

    // synch'ed to avoid explicit invocation upon OOM from colliding with
    // timer task firing
    synchronized void closeIdle(boolean scanAll) {
      long minLastContact = System.currentTimeMillis() - maxIdleTime;
      // concurrent iterator might miss new connections added
      // during the iteration, but that's ok because they won't
      // be idle yet anyway and will be caught on next scan
      int closed = 0;
      for (Connection connection : connections) {
        // stop if connections dropped below threshold unless scanning all
        if (!scanAll && size() < idleScanThreshold) {
          break;
        }
        // stop if not scanning all and max connections are closed
        if (connection.isIdle() &&
            connection.getLastContact() < minLastContact &&
            close(connection) &&
            !scanAll && (++closed == maxIdleToClose)) {
          break;
        }
      }
    }

    void closeAll() {
      // use a copy of the connections to be absolutely sure the concurrent
      // iterator doesn't miss a connection
      for (Connection connection : toArray()) {
        close(connection);
      }
    }

    void startIdleScan() {
      scheduleIdleScanTask();
    }

    void stopIdleScan() {
      idleScanTimer.cancel();
    }

    private void scheduleIdleScanTask() {
      if (!running) {
        return;
      }
      TimerTask idleScanTask = new TimerTask(){
        @Override
        public void run() {
          if (!running) {
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName()+": task running");
          }
          try {
            closeIdle(false);
          } finally {
            // explicitly reschedule so next execution occurs relative
            // to the end of this scan, not the beginning
            scheduleIdleScanTask();
          }
        }
      };
      idleScanTimer.schedule(idleScanTask, idleScanInterval);
    }
  }

}