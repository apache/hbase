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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.MetaNodeTracker;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Tracks the availability of the catalog tables <code>-ROOT-</code> and
 * <code>.META.</code>.
 * <p>
 * This class is "read-only" in that the locations of the catalog tables cannot
 * be explicitly set.  Instead, ZooKeeper is used to learn of the availability
 * and location of ROOT.  ROOT is used to learn of the location of META.  If not
 * available in ROOT, ZooKeeper is used to monitor for a new location of META.
 */
public class CatalogTracker {
  private static final Log LOG = LogFactory.getLog(CatalogTracker.class);

  private final ServerConnection connection;

  private final ZooKeeperWatcher zookeeper;

  private final RootRegionTracker rootRegionTracker;

  private final MetaNodeTracker metaNodeTracker;

  private final AtomicBoolean metaAvailable = new AtomicBoolean(false);
  private HServerAddress metaLocation;

  private final int defaultTimeout;

  public static final byte [] ROOT_REGION =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();

  public static final byte [] META_REGION =
    HRegionInfo.FIRST_META_REGIONINFO.getRegionName();

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables and
   * begin active tracking by executing {@link #start()}.
   * @param zookeeper zk reference
   * @param connection server connection
   * @param abortable if fatal exception
   */
  public CatalogTracker(ZooKeeperWatcher zookeeper, ServerConnection connection,
      Abortable abortable, int defaultTimeout) {
    this.zookeeper = zookeeper;
    this.connection = connection;
    this.rootRegionTracker = new RootRegionTracker(zookeeper, abortable);
    this.metaNodeTracker = new MetaNodeTracker(zookeeper, this);
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Starts the catalog tracker.
   * <p>
   * Determines current availability of catalog tables and ensures all further
   * transitions of either region is tracked.
   * @throws IOException
   */
  public void start() throws IOException {
    // Register listeners with zk
    zookeeper.registerListener(rootRegionTracker);
    zookeeper.registerListener(metaNodeTracker);
    // Start root tracking
    rootRegionTracker.start();
    // Determine meta assignment
    getMetaServerConnection(true);
  }

  /**
   * Gets the current location for <code>-ROOT-</code> or null if location is
   * not currently available.
   * @return location of root, null if not available
   */
  public HServerAddress getRootLocation() {
    return rootRegionTracker.getRootRegionLocation();
  }

  /**
   * Waits indefinitely for availability of <code>-ROOT-</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForRoot()
  throws InterruptedException {
    rootRegionTracker.waitRootRegionLocation(0);
  }

  /**
   * Gets the current location for <code>-ROOT-</code> if available and waits
   * for up to the specified timeout if not immediately available.  Returns null
   * if the timeout elapses before root is available.
   * @param timeout maximum time to wait for root availability, in milliseconds
   * @return location of root
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if root not available before
   *                                          timeout
   */
  public HServerAddress waitForRoot(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    HServerAddress address = rootRegionTracker.waitRootRegionLocation(timeout);
    if (address == null) {
      throw new NotAllMetaRegionsOnlineException(
          "Timed out (" + timeout + "ms)");
    }
    return address;
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForRoot(timeout));
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting for the default timeout specified on instantiation.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForRoot(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * if available.  Returns null if no location is immediately available.
   * @return connection to server hosting root, null if not available
   * @throws IOException
   */
  private HRegionInterface getRootServerConnection()
  throws IOException {
    HServerAddress address = rootRegionTracker.getRootRegionLocation();
    if (address == null) {
      return null;
    }
    return getCachedConnection(address);
  }

  /**
   * Gets a connection to the server currently hosting <code>.META.</code> or
   * null if location is not currently available.
   * <p>
   * If a location is known, a connection to the cached location is returned.
   * If refresh is true, the cached connection is verified first before
   * returning.  If the connection is not valid, it is reset and rechecked.
   * <p>
   * If no location for meta is currently known, method checks ROOT for a new
   * location, verifies META is currently there, and returns a cached connection
   * to the server hosting META.
   *
   * @return connection to server hosting meta, null if location not available
   * @throws IOException
   */
  private HRegionInterface getMetaServerConnection(boolean refresh)
  throws IOException {
    synchronized(metaAvailable) {
      if(metaAvailable.get()) {
        HRegionInterface current = getCachedConnection(metaLocation);
        if(!refresh) {
          return current;
        }
        if(verifyRegionLocation(current, META_REGION)) {
          return current;
        }
        resetMetaLocation();
      }
      HRegionInterface rootConnection = getRootServerConnection();
      if(rootConnection == null) {
        return null;
      }
      HServerAddress newLocation = MetaReader.readMetaLocation(rootConnection);
      if(newLocation == null) {
        return null;
      }
      HRegionInterface newConnection = getCachedConnection(newLocation);
      if(verifyRegionLocation(newConnection, META_REGION)) {
        setMetaLocation(newLocation);
        return newConnection;
      }
      return null;
    }
  }

  /**
   * Waits indefinitely for availability of <code>.META.</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForMeta() throws InterruptedException {
    synchronized(metaAvailable) {
      while(!metaAvailable.get()) {
        metaAvailable.wait();
      }
    }
  }

  /**
   * Gets the current location for <code>.META.</code> if available and waits
   * for up to the specified timeout if not immediately available.  Throws an
   * exception if timed out waiting.
   * @param timeout maximum time to wait for meta availability, in milliseconds
   * @return location of meta
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException unexpected exception connecting to meta server
   * @throws NotAllMetaRegionsOnlineException if meta not available before
   *                                          timeout
   */
  public HServerAddress waitForMeta(long timeout)
  throws InterruptedException, IOException, NotAllMetaRegionsOnlineException {
    long stop = System.currentTimeMillis() + timeout;
    synchronized(metaAvailable) {
      if(getMetaServerConnection(true) != null) {
        return metaLocation;
      }
      while(!metaAvailable.get() &&
          (timeout == 0 || System.currentTimeMillis() < stop)) {
        metaAvailable.wait(timeout);
      }
      if(getMetaServerConnection(true) == null) {
        throw new NotAllMetaRegionsOnlineException(
            "Timed out (" + timeout + "ms");
      }
      return metaLocation;
    }
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForMeta(timeout));
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws NotAllMetaRegionsOnlineException if timed out or interrupted
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForMeta(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  private void resetMetaLocation() {
    LOG.info("Current cached META location is not valid, resetting");
    metaAvailable.set(false);
    metaLocation = null;
  }

  private void setMetaLocation(HServerAddress metaLocation) {
    LOG.info("Found new META location, " + metaLocation);
    metaAvailable.set(true);
    this.metaLocation = metaLocation;
    // no synchronization because these are private and already under lock
    metaAvailable.notifyAll();
  }

  private HRegionInterface getCachedConnection(HServerAddress address)
  throws IOException {
    return connection.getHRegionConnection(address, false);
  }

  private boolean verifyRegionLocation(HRegionInterface metaServer,
      byte [] regionName) {
    try {
      return metaServer.getRegionInfo(regionName) != null;
    } catch (NotServingRegionException e) {
      return false;
    }
  }
}
