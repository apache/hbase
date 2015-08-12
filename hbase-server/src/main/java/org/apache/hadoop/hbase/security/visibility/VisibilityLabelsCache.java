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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.MultiUserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.UserAuthorizations;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Maintains the cache for visibility labels and also uses the zookeeper to update the labels in the
 * system. The cache updation happens based on the data change event that happens on the zookeeper
 * znode for labels table
 */
@InterfaceAudience.Private
public class VisibilityLabelsCache implements VisibilityLabelOrdinalProvider {

  private static final Log LOG = LogFactory.getLog(VisibilityLabelsCache.class);
  private static final int NON_EXIST_LABEL_ORDINAL = 0;
  private static final List<String> EMPTY_LIST = Collections.emptyList();
  private static final Set<Integer> EMPTY_SET = Collections.emptySet();
  private static VisibilityLabelsCache instance;

  private ZKVisibilityLabelWatcher zkVisibilityWatcher;
  private Map<String, Integer> labels = new HashMap<String, Integer>();
  private Map<Integer, String> ordinalVsLabels = new HashMap<Integer, String>();
  private Map<String, Set<Integer>> userAuths = new HashMap<String, Set<Integer>>();
  private Map<String, Set<Integer>> groupAuths = new HashMap<String, Set<Integer>>();

  /**
   * This covers the members labels, ordinalVsLabels and userAuths
   */
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private VisibilityLabelsCache(ZooKeeperWatcher watcher, Configuration conf) throws IOException {
    zkVisibilityWatcher = new ZKVisibilityLabelWatcher(watcher, this, conf);
    try {
      zkVisibilityWatcher.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
      throw new IOException(ke);
    }
  }

  /**
   * Creates the singleton instance, if not yet present, and returns the same.
   * @param watcher
   * @param conf
   * @return Singleton instance of VisibilityLabelsCache
   * @throws IOException
   */
  public synchronized static VisibilityLabelsCache createAndGet(ZooKeeperWatcher watcher,
      Configuration conf) throws IOException {
    // VisibilityLabelService#init() for different regions (in same RS) passes same instance of
    // watcher as all get the instance from RS.
    // watcher != instance.zkVisibilityWatcher.getWatcher() - This check is needed only in UTs with
    // RS restart. It will be same JVM in which RS restarts and instance will be not null. But the
    // watcher associated with existing instance will be stale as the restarted RS will have new
    // watcher with it.
    if (instance == null || watcher != instance.zkVisibilityWatcher.getWatcher()) {
      instance = new VisibilityLabelsCache(watcher, conf);
    }
    return instance;
  }

  /**
   * @return Singleton instance of VisibilityLabelsCache
   * @throws IllegalStateException
   *           when this is called before calling
   *           {@link #createAndGet(ZooKeeperWatcher, Configuration)}
   */
  public static VisibilityLabelsCache get() {
    // By the time this method is called, the singleton instance of VisibilityLabelsCache should
    // have been created.
    if (instance == null) {
      throw new IllegalStateException("VisibilityLabelsCache not yet instantiated");
    }
    return instance;
  }

  public void refreshLabelsCache(byte[] data) throws IOException {
    List<VisibilityLabel> visibilityLabels = null;
    try {
      visibilityLabels = VisibilityUtils.readLabelsFromZKData(data);
    } catch (DeserializationException dse) {
      throw new IOException(dse);
    }
    this.lock.writeLock().lock();
    try {
      labels.clear();
      ordinalVsLabels.clear();
      for (VisibilityLabel visLabel : visibilityLabels) {
        String label = Bytes.toString(visLabel.getLabel().toByteArray());
        labels.put(label, visLabel.getOrdinal());
        ordinalVsLabels.put(visLabel.getOrdinal(), label);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void refreshUserAuthsCache(byte[] data) throws IOException {
    MultiUserAuthorizations multiUserAuths = null;
    try {
      multiUserAuths = VisibilityUtils.readUserAuthsFromZKData(data);
    } catch (DeserializationException dse) {
      throw new IOException(dse);
    }
    this.lock.writeLock().lock();
    try {
      this.userAuths.clear();
      this.groupAuths.clear();
      for (UserAuthorizations userAuths : multiUserAuths.getUserAuthsList()) {
        String user = Bytes.toString(userAuths.getUser().toByteArray());
        if (AuthUtil.isGroupPrincipal(user)) {
          this.groupAuths.put(AuthUtil.getGroupName(user),
            new HashSet<Integer>(userAuths.getAuthList()));
        } else {
          this.userAuths.put(user, new HashSet<Integer>(userAuths.getAuthList()));
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * @param label Not null label string
   * @return The ordinal for the label. The ordinal starts from 1. Returns 0 when passed a non
   *         existing label.
   */
  @Override
  public int getLabelOrdinal(String label) {
    Integer ordinal = null;
    this.lock.readLock().lock();
    try {
      ordinal = labels.get(label);
    } finally {
      this.lock.readLock().unlock();
    }
    if (ordinal != null) {
      return ordinal.intValue();
    }
    // 0 denotes not available
    return NON_EXIST_LABEL_ORDINAL;
  }

  /**
   * @param ordinal The ordinal of label which we are looking for.
   * @return The label having the given ordinal. Returns <code>null</code> when no label exist in
   *         the system with given ordinal
   */
  @Override
  public String getLabel(int ordinal) {
    this.lock.readLock().lock();
    try {
      return this.ordinalVsLabels.get(ordinal);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * @return The total number of visibility labels.
   */
  public int getLabelsCount() {
    this.lock.readLock().lock();
    try {
      return this.labels.size();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public List<String> getUserAuths(String user) {
    this.lock.readLock().lock();
    try {
      List<String> auths = EMPTY_LIST;
      Set<Integer> authOrdinals = getUserAuthsAsOrdinals(user);
      if (!authOrdinals.equals(EMPTY_SET)) {
        auths = new ArrayList<String>(authOrdinals.size());
        for (Integer authOrdinal : authOrdinals) {
          auths.add(ordinalVsLabels.get(authOrdinal));
        }
      }
      return auths;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public List<String> getGroupAuths(String[] groups) {
    this.lock.readLock().lock();
    try {
      List<String> auths = EMPTY_LIST;
      Set<Integer> authOrdinals = getGroupAuthsAsOrdinals(groups);
      if (!authOrdinals.equals(EMPTY_SET)) {
        auths = new ArrayList<String>(authOrdinals.size());
        for (Integer authOrdinal : authOrdinals) {
          auths.add(ordinalVsLabels.get(authOrdinal));
        }
      }
      return auths;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of ordinals of labels associated with the user
   *
   * @param user Not null value.
   * @return the list of ordinals
   */
  public Set<Integer> getUserAuthsAsOrdinals(String user) {
    this.lock.readLock().lock();
    try {
      Set<Integer> auths = userAuths.get(user);
      return (auths == null) ? EMPTY_SET : auths;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of ordinals of labels associated with the groups
   *
   * @param groups
   * @return the list of ordinals
   */
  public Set<Integer> getGroupAuthsAsOrdinals(String[] groups) {
    this.lock.readLock().lock();
    try {
      Set<Integer> authOrdinals = new HashSet<Integer>();
      if (groups != null && groups.length > 0) {
        Set<Integer> groupAuthOrdinals = null;
        for (String group : groups) {
          groupAuthOrdinals = groupAuths.get(group);
          if (groupAuthOrdinals != null && !groupAuthOrdinals.isEmpty()) {
            authOrdinals.addAll(groupAuthOrdinals);
          }
        }
      }
      return (authOrdinals.isEmpty()) ? EMPTY_SET : authOrdinals;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public void writeToZookeeper(byte[] data, boolean labelsOrUserAuths) throws IOException {
    // Update local state, then send it to zookeeper
    if (labelsOrUserAuths) {
      // True for labels
      this.refreshLabelsCache(data);
    } else {
      // False for user auths
      this.refreshUserAuthsCache(data);
    }
    this.zkVisibilityWatcher.writeToZookeeper(data, labelsOrUserAuths);
  }
}
