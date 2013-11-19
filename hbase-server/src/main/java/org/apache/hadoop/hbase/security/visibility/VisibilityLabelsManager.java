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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
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
public class VisibilityLabelsManager {

  private static final Log LOG = LogFactory.getLog(VisibilityLabelsManager.class);
  private static final List<String> EMPTY_LIST = new ArrayList<String>(0);
  private static VisibilityLabelsManager instance;

  private ZKVisibilityLabelWatcher zkVisibilityWatcher;
  private Map<String, Integer> labels = new HashMap<String, Integer>();
  private Map<Integer, String> ordinalVsLabels = new HashMap<Integer, String>();
  private Map<String, Set<Integer>> userAuths = new HashMap<String, Set<Integer>>();
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private VisibilityLabelsManager(ZooKeeperWatcher watcher, Configuration conf) throws IOException {
    zkVisibilityWatcher = new ZKVisibilityLabelWatcher(watcher, this, conf);
    try {
      zkVisibilityWatcher.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
      throw new IOException(ke);
    }
  }

  public synchronized static VisibilityLabelsManager get(ZooKeeperWatcher watcher,
      Configuration conf) throws IOException {
    if (instance == null) {
      instance = new VisibilityLabelsManager(watcher, conf);
    }
    return instance;
  }

  public static VisibilityLabelsManager get() {
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
      for (UserAuthorizations userAuths : multiUserAuths.getUserAuthsList()) {
        String user = Bytes.toString(userAuths.getUser().toByteArray());
        this.userAuths.put(user, new HashSet<Integer>(userAuths.getAuthList()));
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * @param label
   * @return The ordinal for the label. The ordinal starts from 1. Returns 0 when the passed a non
   *         existing label.
   */
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
    return 0;
  }

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
  public int getLabelsCount(){
    return this.labels.size();
  }

  /**
   * @param user
   * @return The labels that the given user is authorized for.
   */
  public List<String> getAuths(String user) {
    List<String> auths = EMPTY_LIST;
    this.lock.readLock().lock();
    try {
      Set<Integer> authOrdinals = userAuths.get(user);
      if (authOrdinals != null) {
        auths = new ArrayList<String>(authOrdinals.size());
        for (Integer authOrdinal : authOrdinals) {
          auths.add(ordinalVsLabels.get(authOrdinal));
        }
      }
    } finally {
      this.lock.readLock().unlock();
    }
    return auths;
  }

  /**
   * Writes the labels data to zookeeper node.
   * @param data
   * @param labelsOrUserAuths true for writing labels and false for user auths.
   */
  public void writeToZookeeper(byte[] data, boolean labelsOrUserAuths) {
    this.zkVisibilityWatcher.writeToZookeeper(data, labelsOrUserAuths);
  }
}
