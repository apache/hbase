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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this class can be used to watch a file for changes. When a file's modification time
 * changes, the callback provided by the user will be called from a background thread. Modification
 * are detected by checking the file's attributes every polling interval. Some things to keep in
 * mind:
 * <ul>
 * <li>The callback should be thread-safe.</li>
 * <li>Changes that happen around the time the thread is started may be missed.</li>
 * <li>There is a delay between a file changing and the callback firing.</li>
 * </ul>
 * <p/>
 * This file was originally copied from the Apache ZooKeeper project, and then modified.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/8148f966947d3ecf3db0b756d93c9ffa88174af9/zookeeper-server/src/main/java/org/apache/zookeeper/common/FileChangeWatcher.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
public final class FileChangeWatcher {

  public interface FileChangeWatcherCallback {
    void callback(Path path);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileChangeWatcher.class);

  enum State {
    NEW, // object created but start() not called yet
    STARTING, // start() called but background thread has not entered main loop
    RUNNING, // background thread is running
    STOPPING, // stop() called but background thread has not exited main loop
    STOPPED // stop() called and background thread has exited, or background thread crashed
  }

  private final WatcherThread watcherThread;
  private State state; // protected by synchronized(this)
  private FileTime lastModifiedTime;
  private final Object lastModifiedTimeLock;
  private final Path filePath;
  private final Duration pollInterval;

  /**
   * Creates a watcher that watches <code>filePath</code> and invokes <code>callback</code> on
   * changes.
   * @param filePath the file to watch.
   * @param callback the callback to invoke with events. <code>event.kind()</code> will return the
   *                 type of event, and <code>event.context()</code> will return the filename
   *                 relative to <code>dirPath</code>.
   * @throws IOException if there is an error creating the WatchService.
   */
  public FileChangeWatcher(Path filePath, String threadNameSuffix, Duration pollInterval,
    FileChangeWatcherCallback callback) throws IOException {
    this.filePath = filePath;
    this.pollInterval = pollInterval;

    state = State.NEW;
    lastModifiedTimeLock = new Object();
    lastModifiedTime = Files.readAttributes(filePath, BasicFileAttributes.class).lastModifiedTime();
    this.watcherThread = new WatcherThread(threadNameSuffix, callback);
    this.watcherThread.setDaemon(true);
  }

  /**
   * Returns the current {@link FileChangeWatcher.State}.
   * @return the current state.
   */
  private synchronized State getState() {
    return state;
  }

  /**
   * Blocks until the current state becomes <code>desiredState</code>. Currently only used by tests,
   * thus package-private.
   * @param desiredState the desired state.
   * @throws InterruptedException if the current thread gets interrupted.
   */
  synchronized void waitForState(State desiredState) throws InterruptedException {
    while (this.state != desiredState) {
      this.wait();
    }
  }

  /**
   * Sets the state to <code>newState</code>.
   * @param newState the new state.
   */
  private synchronized void setState(State newState) {
    state = newState;
    this.notifyAll();
  }

  /**
   * Atomically sets the state to <code>update</code> if and only if the state is currently
   * <code>expected</code>.
   * @param expected the expected state.
   * @param update   the new state.
   * @return true if the update succeeds, or false if the current state does not equal
   *         <code>expected</code>.
   */
  private synchronized boolean compareAndSetState(State expected, State update) {
    if (state == expected) {
      setState(update);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Atomically sets the state to <code>update</code> if and only if the state is currently one of
   * <code>expectedStates</code>.
   * @param expectedStates the expected states.
   * @param update         the new state.
   * @return true if the update succeeds, or false if the current state does not equal any of the
   *         <code>expectedStates</code>.
   */
  private synchronized boolean compareAndSetState(State[] expectedStates, State update) {
    for (State expected : expectedStates) {
      if (state == expected) {
        setState(update);
        return true;
      }
    }
    return false;
  }

  /**
   * Tells the background thread to start. Does not wait for it to be running. Calling this method
   * more than once has no effect.
   */
  public void start() {
    if (!compareAndSetState(State.NEW, State.STARTING)) {
      // If previous state was not NEW, start() has already been called.
      return;
    }
    this.watcherThread.start();
  }

  /**
   * Tells the background thread to stop. Does not wait for it to exit.
   */
  public void stop() {
    if (compareAndSetState(new State[] { State.RUNNING, State.STARTING }, State.STOPPING)) {
      watcherThread.interrupt();
    }
  }

  String getWatcherThreadName() {
    return watcherThread.getName();
  }

  private static void handleException(Thread thread, Throwable e) {
    LOG.warn("Exception occurred from thread {}", thread.getName(), e);
  }

  /**
   * Inner class that implements the watcher thread logic.
   */
  private class WatcherThread extends Thread {

    private static final String THREAD_NAME_PREFIX = "FileChangeWatcher-";

    final FileChangeWatcherCallback callback;

    WatcherThread(String threadNameSuffix, FileChangeWatcherCallback callback) {
      super(THREAD_NAME_PREFIX + threadNameSuffix);
      this.callback = callback;
      setUncaughtExceptionHandler(FileChangeWatcher::handleException);
    }

    @Override
    public void run() {
      try {
        LOG.debug("{} thread started", getName());
        if (
          !compareAndSetState(FileChangeWatcher.State.STARTING, FileChangeWatcher.State.RUNNING)
        ) {
          // stop() called shortly after start(), before
          // this thread started running.
          FileChangeWatcher.State state = FileChangeWatcher.this.getState();
          if (state != FileChangeWatcher.State.STOPPING) {
            throw new IllegalStateException("Unexpected state: " + state);
          }
          return;
        }
        runLoop();
      } catch (Exception e) {
        LOG.warn("Error in runLoop()", e);
        throw new RuntimeException(e);
      } finally {
        LOG.debug("{} thread finished", getName());
        FileChangeWatcher.this.setState(FileChangeWatcher.State.STOPPED);
      }
    }

    private void runLoop() throws IOException {
      while (FileChangeWatcher.this.getState() == FileChangeWatcher.State.RUNNING) {
        BasicFileAttributes attributes = Files.readAttributes(filePath, BasicFileAttributes.class);
        boolean modified = false;
        synchronized (lastModifiedTimeLock) {
          FileTime maybeNewLastModifiedTime = attributes.lastModifiedTime();
          if (!lastModifiedTime.equals(maybeNewLastModifiedTime)) {
            modified = true;
            lastModifiedTime = maybeNewLastModifiedTime;
          }
        }

        // avoid calling callback while holding lock
        if (modified) {
          try {
            callback.callback(filePath);
          } catch (Throwable e) {
            LOG.error("Error from callback", e);
          }
        }

        try {
          Thread.sleep(pollInterval.toMillis());
        } catch (InterruptedException e) {
          LOG.debug("Interrupted", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }
}
