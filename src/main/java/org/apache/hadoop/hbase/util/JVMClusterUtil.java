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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

/**
 * Utility used running a cluster all in the one JVM.
 */
public class JVMClusterUtil {
  private static final Log LOG = LogFactory.getLog(JVMClusterUtil.class);

  /**
   * Datastructure to hold RegionServer Thread and RegionServer instance
   */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;

    public RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RegionServer:" + index);
      this.regionServer = r;
    }

    /** @return the region server */
    public HRegionServer getRegionServer() {
      return this.regionServer;
    }

    /**
     * Block until the region server has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      // The server is marked online after the init method completes inside of
      // the HRS#run method.  HRS#init can fail for whatever region.  In those
      // cases, we'll jump out of the run without setting online flag.  Check
      // stopRequested so we don't wait here a flag that will never be flipped.
      while (!this.regionServer.isOnline() &&
          !this.regionServer.isStopRequested()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue waiting
        }
      }
    }
  }

  /**
   * Creates a {@link RegionServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hrsc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws IOException
   * @return Region server added.
   */
  public static JVMClusterUtil.RegionServerThread createRegionServerThread(
      final Configuration c, final Class<? extends HRegionServer> hrsc,
      final int index)
  throws IOException {
      HRegionServer server;
      try {
        server = hrsc.getConstructor(Configuration.class).newInstance(c);
        server.initialize();
      } catch (Exception e) {
        IOException ioe = new IOException();
        ioe.initCause(e);
        throw ioe;
      }
      return new JVMClusterUtil.RegionServerThread(server, index);
  }

  /**
   * Creates a {@link HMaster}. Call 'start' on the returned thread to make it
   * run. Modifies the passed configuration -- the caller is responsible for
   * defensive copying. 
   * @param masterConf configuration to use
   * @param hmc class to create an instance of
   * @param masterId a unique identifier of a master within a mini-cluster
   * @return the new master
   */
  public static HMaster createMaster(
      final Configuration masterConf, final Class<? extends HMaster> hmc)
  throws IOException {
    LOG.debug("Creating a new master, class: " + hmc.getName());

    HMaster master;
    try {
      master = hmc.getConstructor(Configuration.class).newInstance(masterConf);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of Master: " +
        hmc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return master;
  }

  /**
   * Start the cluster.
   * @param m
   * @param regionServers
   * @return Address to use contacting master.
   */
  public static String startup(final List<HMaster> masters,
      final List<JVMClusterUtil.RegionServerThread> regionservers) throws IOException {
    if (masters != null) {
      for (HMaster t : masters) {
        t.start();
      }
    }
    if (regionservers != null) {
      for (JVMClusterUtil.RegionServerThread t: regionservers) {
        t.start();
      }
    }
    if (masters == null || masters.isEmpty()) {
      return null;
    }
    // Wait for an active master
    while (true) {
      for (HMaster t : masters) {
        if (t.isActiveMaster()) {
          return t.getServerName().toString();
        }
      }
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        // Keep waiting
      }
    }
  }

  /**
   * @param master
   * @param regionservers
   */
  public static void shutdown(final List<HMaster> masters,
      final List<RegionServerThread> regionservers) {
    LOG.debug("Shutting down HBase Cluster");
    if (masters != null) {
      for (HMaster t : masters) {
        if (t.isActiveMaster()) {
          t.requestClusterShutdown();
        } else {
          // This will only stop this particular master.
          t.stop("normal shutdown");
        }
      }
    }

    boolean interrupted = false;
    try {
      // regionServerThreads can never be null because they are initialized when
      // the class is constructed.
      for (Thread t : regionservers) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
  
      if (masters != null) {
        for (HMaster t : masters) {
          while (t.isAlive()) {
            try {
              t.join();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    LOG.info("Shutdown of " +
        ((masters != null) ? masters.size() : "0") + " master(s) and " +
        ((regionservers != null) ? regionservers.size() : "0") +
        " regionserver(s) complete");
  }
}
