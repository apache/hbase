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

package org.apache.hadoop.hbase.security.token;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKLeaderManager;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages an internal list of secret keys used to sign new authentication
 * tokens as they are generated, and to valid existing tokens used for
 * authentication.
 *
 * <p>
 * A single instance of {@code AuthenticationTokenSecretManager} will be
 * running as the "leader" in a given HBase cluster.  The leader is responsible
 * for periodically generating new secret keys, which are then distributed to
 * followers via ZooKeeper, and for expiring previously used secret keys that
 * are no longer needed (as any tokens using them have expired).
 * </p>
 */
@InterfaceAudience.Private
public class AuthenticationTokenSecretManager
    extends SecretManager<AuthenticationTokenIdentifier> {

  static final String NAME_PREFIX = "SecretManager-";

  private static final Logger LOG = LoggerFactory.getLogger(
      AuthenticationTokenSecretManager.class);

  private long lastKeyUpdate;
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private ZKSecretWatcher zkWatcher;
  private LeaderElector leaderElector;
  private ZKClusterId clusterId;

  private Map<Integer,AuthenticationKey> allKeys = new ConcurrentHashMap<>();
  private AuthenticationKey currentKey;

  private int idSeq;
  private AtomicLong tokenSeq = new AtomicLong();
  private String name;

  /**
   * Create a new secret manager instance for generating keys.
   * @param conf Configuration to use
   * @param zk Connection to zookeeper for handling leader elections
   * @param keyUpdateInterval Time (in milliseconds) between rolling a new master key for token signing
   * @param tokenMaxLifetime Maximum age (in milliseconds) before a token expires and is no longer valid
   */
  /* TODO: Restrict access to this constructor to make rogues instances more difficult.
   * For the moment this class is instantiated from
   * org.apache.hadoop.hbase.ipc.SecureServer so public access is needed.
   */
  public AuthenticationTokenSecretManager(Configuration conf,
                                          ZKWatcher zk, String serverName,
                                          long keyUpdateInterval, long tokenMaxLifetime) {
    this.zkWatcher = new ZKSecretWatcher(conf, zk, this);
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenMaxLifetime = tokenMaxLifetime;
    this.leaderElector = new LeaderElector(zk, serverName);
    this.name = NAME_PREFIX+serverName;
    this.clusterId = new ZKClusterId(zk, zk);
  }

  public void start() {
    try {
      // populate any existing keys
      this.zkWatcher.start();
      // try to become leader
      this.leaderElector.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
    }
  }

  public void stop() {
    this.leaderElector.stop("SecretManager stopping");
  }

  public boolean isMaster() {
    return leaderElector.isMaster();
  }

  public String getName() {
    return name;
  }

  @Override
  protected synchronized byte[] createPassword(AuthenticationTokenIdentifier identifier) {
    long now = EnvironmentEdgeManager.currentTime();
    AuthenticationKey secretKey = currentKey;
    identifier.setKeyId(secretKey.getKeyId());
    identifier.setIssueDate(now);
    identifier.setExpirationDate(now + tokenMaxLifetime);
    identifier.setSequenceNumber(tokenSeq.getAndIncrement());
    return createPassword(identifier.getBytes(),
        secretKey.getKey());
  }

  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier identifier)
      throws InvalidToken {
    long now = EnvironmentEdgeManager.currentTime();
    if (identifier.getExpirationDate() < now) {
      throw new InvalidToken("Token has expired");
    }
    AuthenticationKey masterKey = allKeys.get(identifier.getKeyId());
    if(masterKey == null) {
      if(zkWatcher.getWatcher().isAborted()) {
        LOG.error("ZKWatcher is abort");
        throw new InvalidToken("Token keys could not be sync from zookeeper"
            + " because of ZKWatcher abort");
      }
      synchronized (this) {
        if (!leaderElector.isAlive() || leaderElector.isStopped()) {
          LOG.warn("Thread leaderElector[" + leaderElector.getName() + ":"
              + leaderElector.getId() + "] is stopped or not alive");
          leaderElector.start();
          LOG.info("Thread leaderElector [" + leaderElector.getName() + ":"
              + leaderElector.getId() + "] is started");
        }
      }
      zkWatcher.refreshKeys();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sync token keys from zookeeper");
      }
      masterKey = allKeys.get(identifier.getKeyId());
    }
    if (masterKey == null) {
      throw new InvalidToken("Unknown master key for token (id="+
          identifier.getKeyId()+")");
    }
    // regenerate the password
    return createPassword(identifier.getBytes(),
        masterKey.getKey());
  }

  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return new AuthenticationTokenIdentifier();
  }

  public Token<AuthenticationTokenIdentifier> generateToken(String username) {
    AuthenticationTokenIdentifier ident =
        new AuthenticationTokenIdentifier(username);
    Token<AuthenticationTokenIdentifier> token = new Token<>(ident, this);
    if (clusterId.hasId()) {
      token.setService(new Text(clusterId.getId()));
    }
    return token;
  }

  public synchronized void addKey(AuthenticationKey key) throws IOException {
    // ignore zk changes when running as master
    if (leaderElector.isMaster()) {
      LOG.debug("Running as master, ignoring new key {}", key);
      return;
    }

    LOG.debug("Adding key {}", key.getKeyId());

    allKeys.put(key.getKeyId(), key);
    if (currentKey == null || key.getKeyId() > currentKey.getKeyId()) {
      currentKey = key;
    }
    // update current sequence
    if (key.getKeyId() > idSeq) {
      idSeq = key.getKeyId();
    }
  }

  synchronized boolean removeKey(Integer keyId) {
    // ignore zk changes when running as master
    if (leaderElector.isMaster()) {
      LOG.debug("Running as master, ignoring removed keyid={}", keyId);
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing keyid={}", keyId);
    }

    allKeys.remove(keyId);
    return true;
  }

  synchronized AuthenticationKey getCurrentKey() {
    return currentKey;
  }

  AuthenticationKey getKey(int keyId) {
    return allKeys.get(keyId);
  }

  synchronized void removeExpiredKeys() {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping removeExpiredKeys() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTime();
    Iterator<AuthenticationKey> iter = allKeys.values().iterator();
    while (iter.hasNext()) {
      AuthenticationKey key = iter.next();
      if (key.getExpiration() < now) {
        LOG.debug("Removing expired key {}", key);
        iter.remove();
        zkWatcher.removeKeyFromZK(key);
      }
    }
  }

  synchronized boolean isCurrentKeyRolled() {
    return currentKey != null;
  }

  synchronized void rollCurrentKey() {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping rollCurrentKey() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTime();
    AuthenticationKey prev = currentKey;
    AuthenticationKey newKey = new AuthenticationKey(++idSeq,
        Long.MAX_VALUE, // don't allow to expire until it's replaced by a new key
        generateSecret());
    allKeys.put(newKey.getKeyId(), newKey);
    currentKey = newKey;
    zkWatcher.addKeyToZK(newKey);
    lastKeyUpdate = now;

    if (prev != null) {
      // make sure previous key is still stored
      prev.setExpiration(now + tokenMaxLifetime);
      allKeys.put(prev.getKeyId(), prev);
      zkWatcher.updateKeyInZK(prev);
    }
  }

  synchronized long getLastKeyUpdate() {
    return lastKeyUpdate;
  }

  public static SecretKey createSecretKey(byte[] raw) {
    return SecretManager.createSecretKey(raw);
  }

  private class LeaderElector extends Thread implements Stoppable {
    private boolean stopped = false;
    /** Flag indicating whether we're in charge of rolling/expiring keys */
    private boolean isMaster = false;
    private ZKLeaderManager zkLeader;

    public LeaderElector(ZKWatcher watcher, String serverName) {
      setDaemon(true);
      setName("ZKSecretWatcher-leaderElector");
      zkLeader = new ZKLeaderManager(watcher,
          ZNodePaths.joinZNode(zkWatcher.getRootKeyZNode(), "keymaster"),
          Bytes.toBytes(serverName), this);
    }

    public boolean isMaster() {
      return isMaster;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }

    @Override
    public void stop(String reason) {
      if (stopped) {
        return;
      }

      stopped = true;
      // prevent further key generation when stopping
      if (isMaster) {
        zkLeader.stepDownAsLeader();
      }
      isMaster = false;
      LOG.info("Stopping leader election, because: "+reason);
      interrupt();
    }

    @Override
    public void run() {
      zkLeader.start();
      zkLeader.waitToBecomeLeader();
      isMaster = true;

      while (!stopped) {
        long now = EnvironmentEdgeManager.currentTime();

        // clear any expired
        removeExpiredKeys();
        long localLastKeyUpdate = getLastKeyUpdate();
        if (localLastKeyUpdate + keyUpdateInterval < now) {
          // roll a new master key
          rollCurrentKey();
        }

        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted waiting for next update", ie);
          }
        }
      }
    }
  }
}
