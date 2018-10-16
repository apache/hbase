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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;

/**
 * Sends responses of RPC back to clients.
 */
@InterfaceAudience.Private
class SimpleRpcServerResponder extends Thread {

  private final SimpleRpcServer simpleRpcServer;
  private final Selector writeSelector;
  private final Set<SimpleServerRpcConnection> writingCons =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  SimpleRpcServerResponder(SimpleRpcServer simpleRpcServer) throws IOException {
    this.simpleRpcServer = simpleRpcServer;
    this.setName("RpcServer.responder");
    this.setDaemon(true);
    this.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
    writeSelector = Selector.open(); // create a selector
  }

  @Override
  public void run() {
    SimpleRpcServer.LOG.debug(getName() + ": starting");
    try {
      doRunLoop();
    } finally {
      SimpleRpcServer.LOG.info(getName() + ": stopping");
      try {
        writeSelector.close();
      } catch (IOException ioe) {
        SimpleRpcServer.LOG.error(getName() + ": couldn't close write selector", ioe);
      }
    }
  }

  /**
   * Take the list of the connections that want to write, and register them in the selector.
   */
  private void registerWrites() {
    Iterator<SimpleServerRpcConnection> it = writingCons.iterator();
    while (it.hasNext()) {
      SimpleServerRpcConnection c = it.next();
      it.remove();
      SelectionKey sk = c.channel.keyFor(writeSelector);
      try {
        if (sk == null) {
          try {
            c.channel.register(writeSelector, SelectionKey.OP_WRITE, c);
          } catch (ClosedChannelException e) {
            // ignore: the client went away.
            if (SimpleRpcServer.LOG.isTraceEnabled()) SimpleRpcServer.LOG.trace("ignored", e);
          }
        } else {
          sk.interestOps(SelectionKey.OP_WRITE);
        }
      } catch (CancelledKeyException e) {
        // ignore: the client went away.
        if (SimpleRpcServer.LOG.isTraceEnabled()) SimpleRpcServer.LOG.trace("ignored", e);
      }
    }
  }

  /**
   * Add a connection to the list that want to write,
   */
  public void registerForWrite(SimpleServerRpcConnection c) {
    if (writingCons.add(c)) {
      writeSelector.wakeup();
    }
  }

  private void doRunLoop() {
    long lastPurgeTime = 0; // last check for old calls.
    while (this.simpleRpcServer.running) {
      try {
        registerWrites();
        int keyCt = writeSelector.select(this.simpleRpcServer.purgeTimeout);
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
            SimpleRpcServer.LOG.debug(getName() + ": asyncWrite", e);
          }
        }

        lastPurgeTime = purge(lastPurgeTime);

      } catch (OutOfMemoryError e) {
        if (this.simpleRpcServer.errorHandler != null) {
          if (this.simpleRpcServer.errorHandler.checkOOME(e)) {
            SimpleRpcServer.LOG.info(getName() + ": exiting on OutOfMemoryError");
            return;
          }
        } else {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          SimpleRpcServer.LOG.warn(getName() + ": OutOfMemoryError in server select", e);
          try {
            Thread.sleep(60000);
          } catch (InterruptedException ex) {
            SimpleRpcServer.LOG.debug("Interrupted while sleeping");
            return;
          }
        }
      } catch (Exception e) {
        SimpleRpcServer.LOG
            .warn(getName() + ": exception in Responder " + StringUtils.stringifyException(e), e);
      }
    }
    SimpleRpcServer.LOG.info(getName() + ": stopped");
  }

  /**
   * If there were some calls that have not been sent out for a long time, we close the connection.
   * @return the time of the purge.
   */
  private long purge(long lastPurgeTime) {
    long now = System.currentTimeMillis();
    if (now < lastPurgeTime + this.simpleRpcServer.purgeTimeout) {
      return lastPurgeTime;
    }

    ArrayList<SimpleServerRpcConnection> conWithOldCalls = new ArrayList<>();
    // get the list of channels from list of keys.
    synchronized (writeSelector.keys()) {
      for (SelectionKey key : writeSelector.keys()) {
        SimpleServerRpcConnection connection = (SimpleServerRpcConnection) key.attachment();
        if (connection == null) {
          throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
        }
        if (connection.lastSentTime > 0 &&
            now > connection.lastSentTime + this.simpleRpcServer.purgeTimeout) {
          conWithOldCalls.add(connection);
        }
      }
    }

    // Seems safer to close the connection outside of the synchronized loop...
    for (SimpleServerRpcConnection connection : conWithOldCalls) {
      this.simpleRpcServer.closeConnection(connection);
    }

    return now;
  }

  private void doAsyncWrite(SelectionKey key) throws IOException {
    SimpleServerRpcConnection connection = (SimpleServerRpcConnection) key.attachment();
    if (connection == null) {
      throw new IOException("doAsyncWrite: no connection");
    }
    if (key.channel() != connection.channel) {
      throw new IOException("doAsyncWrite: bad channel");
    }

    if (processAllResponses(connection)) {
      try {
        // We wrote everything, so we don't need to be told when the socket is ready for
        // write anymore.
        key.interestOps(0);
      } catch (CancelledKeyException e) {
        /*
         * The Listener/reader might have closed the socket. We don't explicitly cancel the key, so
         * not sure if this will ever fire. This warning could be removed.
         */
        SimpleRpcServer.LOG.warn("Exception while changing ops : " + e);
      }
    }
  }

  /**
   * Process the response for this call. You need to have the lock on
   * {@link org.apache.hadoop.hbase.ipc.SimpleServerRpcConnection#responseWriteLock}
   * @return true if we proceed the call fully, false otherwise.
   * @throws IOException
   */
  private boolean processResponse(SimpleServerRpcConnection conn, RpcResponse resp)
      throws IOException {
    boolean error = true;
    BufferChain buf = resp.getResponse();
    try {
      // Send as much data as we can in the non-blocking fashion
      long numBytes =
          this.simpleRpcServer.channelWrite(conn.channel, buf);
      if (numBytes < 0) {
        throw new HBaseIOException("Error writing on the socket " + conn);
      }
      error = false;
    } finally {
      if (error) {
        SimpleRpcServer.LOG.debug(conn + ": output error -- closing");
        // We will be closing this connection itself. Mark this call as done so that all the
        // buffer(s) it got from pool can get released
        resp.done();
        this.simpleRpcServer.closeConnection(conn);
      }
    }

    if (!buf.hasRemaining()) {
      resp.done();
      return true;
    } else {
      // set the serve time when the response has to be sent later
      conn.lastSentTime = System.currentTimeMillis();
      return false; // Socket can't take more, we will have to come back.
    }
  }

  /**
   * Process all the responses for this connection
   * @return true if all the calls were processed or that someone else is doing it. false if there *
   *         is still some work to do. In this case, we expect the caller to delay us.
   * @throws IOException
   */
  private boolean processAllResponses(final SimpleServerRpcConnection connection)
      throws IOException {
    // We want only one writer on the channel for a connection at a time.
    connection.responseWriteLock.lock();
    try {
      for (int i = 0; i < 20; i++) {
        // protection if some handlers manage to need all the responder
        RpcResponse resp = connection.responseQueue.pollFirst();
        if (resp == null) {
          return true;
        }
        if (!processResponse(connection, resp)) {
          connection.responseQueue.addFirst(resp);
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
  void doRespond(SimpleServerRpcConnection conn, RpcResponse resp) throws IOException {
    boolean added = false;
    // If there is already a write in progress, we don't wait. This allows to free the handlers
    // immediately for other tasks.
    if (conn.responseQueue.isEmpty() && conn.responseWriteLock.tryLock()) {
      try {
        if (conn.responseQueue.isEmpty()) {
          // If we're alone, we can try to do a direct call to the socket. It's
          // an optimization to save on context switches and data transfer between cores..
          if (processResponse(conn, resp)) {
            return; // we're done.
          }
          // Too big to fit, putting ahead.
          conn.responseQueue.addFirst(resp);
          added = true; // We will register to the selector later, outside of the lock.
        }
      } finally {
        conn.responseWriteLock.unlock();
      }
    }

    if (!added) {
      conn.responseQueue.addLast(resp);
    }
    registerForWrite(conn);
  }
}
