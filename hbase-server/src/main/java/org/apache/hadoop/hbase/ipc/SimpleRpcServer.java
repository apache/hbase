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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
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
        this.pendingConnections = new LinkedBlockingQueue<>(readerPendingConnectionQueueLength);
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
          } catch (CancelledKeyException e) {
            LOG.error(getName() + ": CancelledKeyException in Reader", e);
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

      ArrayList<Connection> conWithOldCalls = new ArrayList<>();
      // get the list of channels from list of keys.
      synchronized (writeSelector.keys()) {
        for (SelectionKey key : writeSelector.keys()) {
          Connection connection = (Connection) key.attachment();
          if (connection == null) {
            throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
          }
          SimpleServerCall call = connection.responseQueue.peekFirst();
          if (call != null && now > call.lastSentTime + purgeTimeout) {
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
    private boolean processResponse(final SimpleServerCall call) throws IOException {
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
          SimpleServerCall call = connection.responseQueue.pollFirst();
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
    void doRespond(SimpleServerCall call) throws IOException {
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
      call.lastSentTime = System.currentTimeMillis();
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
    protected final ConcurrentLinkedDeque<SimpleServerCall> responseQueue = new ConcurrentLinkedDeque<>();
    private final Lock responseWriteLock = new ReentrantLock();
    private LongAdder rpcCount = new LongAdder(); // number of outstanding rpcs
    private long lastContact;
    protected Socket socket;

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
      this.saslCall = new SimpleServerCall(SASL_CALLID, null, null, null, null, null, this, 0, null,
          null, System.currentTimeMillis(), 0, reservoir, cellBlockBuilder, null, responder);
      this.setConnectionHeaderResponseCall = new SimpleServerCall(CONNECTION_HEADER_RESPONSE_CALLID,
          null, null, null, null, null, this, 0, null, null, System.currentTimeMillis(), 0,
          reservoir, cellBlockBuilder, null, responder);
      this.authFailedCall = new SimpleServerCall(AUTHORIZATION_FAILED_CALLID, null, null, null,
          null, null, this, 0, null, null, System.currentTimeMillis(), 0, reservoir,
          cellBlockBuilder, null, responder);
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
          authFailedCall.sendResponseIfReady();
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
            SimpleServerCall reqTooBig = new SimpleServerCall(header.getCallId(), this.service,
                null, null, null, null, this, 0, null, this.addr, System.currentTimeMillis(), 0,
                reservoir, cellBlockBuilder, null, responder);
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
      SimpleServerCall fakeCall = new SimpleServerCall(-1, null, null, null, null, null, this, -1,
          null, null, System.currentTimeMillis(), 0, reservoir, cellBlockBuilder, null, responder);
      setupResponse(null, fakeCall, e, msg);
      responder.doRespond(fakeCall);
      // Returning -1 closes out the connection.
      return -1;
    }

    @Override
    public synchronized void close() {
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

    @Override
    public ServerCall createCall(int id, final BlockingService service, final MethodDescriptor md,
        RequestHeader header, Message param, CellScanner cellScanner,
        RpcServer.Connection connection, long size, TraceInfo tinfo,
        final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
      return new SimpleServerCall(id, service, md, header, param, cellScanner, connection, size,
          tinfo, remoteAddress, System.currentTimeMillis(), timeout, reservoir, cellBlockBuilder,
          reqCleanup, responder);
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

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
      throws IOException {
    return call(service, md, param, cellScanner, receiveTime, status, System.currentTimeMillis(),
      0);
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status,
      long startTime, int timeout) throws IOException {
    SimpleServerCall fakeCall = new SimpleServerCall(-1, service, md, null, param, cellScanner,
        null, -1, null, null, receiveTime, timeout, reservoir, cellBlockBuilder, null, null);
    return call(fakeCall, status);
  }

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
          if (LOG.isTraceEnabled()) {
            LOG.trace("running");
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