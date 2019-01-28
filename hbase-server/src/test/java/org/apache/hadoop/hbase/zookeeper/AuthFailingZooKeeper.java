/*
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A wrapper around {@link ZooKeeper} which tries to mimic semantics around AUTH_FAILED. When
 * an AuthFailedException is thrown the first time, it is thrown every time after that.
 */
public class AuthFailingZooKeeper extends ZooKeeper {
  private static final AuthFailedException AUTH_FAILED_EXCEPTION = new AuthFailedException();

  // Latch for the "first" AUTH_FAILED occurrence
  private final AtomicBoolean FAILURE_LATCH = new AtomicBoolean(false);
  // Latch for when we start always throwing AUTH_FAILED
  private final AtomicBoolean IS_AUTH_FAILED = new AtomicBoolean(false);

  public AuthFailingZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException {
    super(connectString, sessionTimeout, watcher);
  }

  /**
   * Causes AUTH_FAILED exceptions to be thrown by {@code this}.
   */
  public void triggerAuthFailed() {
    FAILURE_LATCH.set(true);
  }

  void check() throws KeeperException {
    // ZK state model states that once an AUTH_FAILED exception is thrown, it is thrown for
    // every subsequent operation
    if (IS_AUTH_FAILED.get()) {
      throw AUTH_FAILED_EXCEPTION;
    }
    // We're not yet throwing AUTH_FAILED
    if (!FAILURE_LATCH.get()) {
      return;
    }
    // Start throwing AUTH_FAILED
    IS_AUTH_FAILED.set(true);
    throw AUTH_FAILED_EXCEPTION;
  }

  @Override
  public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException,
      InterruptedException {
    check();
    return super.getData(path, watcher, stat);
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl, CreateMode cmode)
      throws KeeperException, InterruptedException {
    check();
    return super.create(path,  data, acl, cmode);
  }

  @Override
  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    check();
    return super.exists(path, watch);
  }

  @Override
  public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
    check();
    return super.exists(path, watcher);
  }

  @Override
  public List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException {
    check();
    return super.getChildren(path, watch);
  }
}
