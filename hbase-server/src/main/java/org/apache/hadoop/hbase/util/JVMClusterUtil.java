/**
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

import java.io.InterruptedIOException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

/**
 * Utility used running a cluster all in the one JVM.
 */
@InterfaceAudience.Private
public class JVMClusterUtil {
  private static final Log LOG = LogFactory.getLog(JVMClusterUtil.class);

  /**
   * Datastructure to hold RegionServer Thread and RegionServer instance
   */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;

    public RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RS:" + index + ";" + r.getServerName().toShortString());
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
      regionServer.waitForServerOnline();
    }
  }

  /**
   * Creates a {@link RegionServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param cp consensus provider to use
   * @param hrsc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws IOException
   * @return Region server added.
   */
  public static JVMClusterUtil.RegionServerThread createRegionServerThread(
      final Configuration c, CoordinatedStateManager cp, final Class<? extends HRegionServer> hrsc,
      final int index)
  throws IOException {
    HRegionServer server;
    try {

      Constructor<? extends HRegionServer> ctor = hrsc.getConstructor(Configuration.class,
      CoordinatedStateManager.class);
      ctor.setAccessible(true);
      server = ctor.newInstance(c, cp);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of RegionServer: " +
        hrsc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new JVMClusterUtil.RegionServerThread(server, index);
  }


  /**
   * Datastructure to hold Master Thread and Master instance
   */
  public static class MasterThread extends Thread {
    private final HMaster master;

    public MasterThread(final HMaster m, final int index) {
      super(m, "M:" + index + ";" + m.getServerName().toShortString());
      this.master = m;
    }

    /** @return the master */
    public HMaster getMaster() {
      return this.master;
    }
  }

  /**
   * Creates a {@link MasterThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param cp consensus provider to use
   * @param hmc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws IOException
   * @return Master added.
   */
  public static JVMClusterUtil.MasterThread createMasterThread(
      final Configuration c, CoordinatedStateManager cp, final Class<? extends HMaster> hmc,
      final int index)
  throws IOException {
    HMaster server;
    try {
      server = hmc.getConstructor(Configuration.class, CoordinatedStateManager.class).
        newInstance(c, cp);
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
    return new JVMClusterUtil.MasterThread(server, index);
  }

  private static JVMClusterUtil.MasterThread findActiveMaster(
    List<JVMClusterUtil.MasterThread> masters) {
    for (JVMClusterUtil.MasterThread t : masters) {
      if (t.master.isActiveMaster()) {
        return t;
      }
    }

    return null;
  }

  /**
   * Start the cluster.  Waits until there is a primary master initialized
   * and returns its address.
   * @param masters
   * @param regionservers
   * @return Address to use contacting primary master.
   */
  public static String startup(final List<JVMClusterUtil.MasterThread> masters,
      final List<JVMClusterUtil.RegionServerThread> regionservers) throws IOException {

    if (masters == null || masters.isEmpty()) {
      return null;
    }

    for (JVMClusterUtil.MasterThread t : masters) {
      t.start();
    }

    // Wait for an active master
    //  having an active master before starting the region threads allows
    //  then to succeed on their connection to master
    long startTime = System.currentTimeMillis();
    while (findActiveMaster(masters) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      if (System.currentTimeMillis() > startTime + 30000) {
        throw new RuntimeException("Master not active after 30 seconds");
      }
    }

    if (regionservers != null) {
      for (JVMClusterUtil.RegionServerThread t: regionservers) {
        t.start();
      }
    }

    // Wait for an active master to be initialized (implies being master)
    //  with this, when we return the cluster is complete
    startTime = System.currentTimeMillis();
    final int maxwait = 200000;
    while (true) {
      JVMClusterUtil.MasterThread t = findActiveMaster(masters);
      if (t != null && t.master.isInitialized()) {
        return t.master.getServerName().toString();
      }
      // REMOVE
      if (System.currentTimeMillis() > startTime + 10000) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
      if (System.currentTimeMillis() > startTime + maxwait) {
        String msg = "Master not initialized after " + maxwait + "ms seconds";
        Threads.printThreadInfo(System.out,
          "Thread dump because: " + msg);
        throw new RuntimeException(msg);
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
  }

  /**
   * @param masters
   * @param regionservers
   */
  public static void shutdown(final List<MasterThread> masters,
      final List<RegionServerThread> regionservers) {
    LOG.debug("Shutting down HBase Cluster");
    if (masters != null) {
      // Do backups first.
      JVMClusterUtil.MasterThread activeMaster = null;
      for (JVMClusterUtil.MasterThread t : masters) {
        if (!t.master.isActiveMaster()) {
          t.master.stopMaster();
        } else {
          activeMaster = t;
        }
      }
      // Do active after.
      if (activeMaster != null)
        activeMaster.master.shutdown();

    }
    boolean wasInterrupted = false;
    final long maxTime = System.currentTimeMillis() + 30 * 1000;
    if (regionservers != null) {
      // first try nicely.
      for (RegionServerThread t : regionservers) {
        t.getRegionServer().stop("Shutdown requested");
      }
      for (RegionServerThread t : regionservers) {
        long now = System.currentTimeMillis();
        if (t.isAlive() && !wasInterrupted && now < maxTime) {
          try {
            t.join(maxTime - now);
          } catch (InterruptedException e) {
            LOG.info("Got InterruptedException on shutdown - " +
                "not waiting anymore on region server ends", e);
            wasInterrupted = true; // someone wants us to speed up.
          }
        }
      }

      // Let's try to interrupt the remaining threads if any.
      for (int i = 0; i < 100; ++i) {
        boolean atLeastOneLiveServer = false;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            atLeastOneLiveServer = true;
            try {
              LOG.warn("RegionServerThreads remaining, give one more chance before interrupting");
              t.join(1000);
            } catch (InterruptedException e) {
              wasInterrupted = true;
            }
          }
        }
        if (!atLeastOneLiveServer) break;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            LOG.warn("RegionServerThreads taking too long to stop, interrupting");
            t.interrupt();
          }
        }
      }
    }

    if (masters != null) {
      for (JVMClusterUtil.MasterThread t : masters) {
        while (t.master.isAlive() && !wasInterrupted) {
          try {
            // The below has been replaced to debug sometime hangs on end of
            // tests.
            // this.master.join():
            Threads.threadDumpingIsAlive(t.master.getThread());
          } catch(InterruptedException e) {
            LOG.info("Got InterruptedException on shutdown - " +
                "not waiting anymore on master ends", e);
            wasInterrupted = true;
          }
        }
      }
    }
    LOG.info("Shutdown of " +
      ((masters != null) ? masters.size() : "0") + " master(s) and " +
      ((regionservers != null) ? regionservers.size() : "0") +
      " regionserver(s) " + (wasInterrupted ? "interrupted" : "complete"));

    if (wasInterrupted){
      Thread.currentThread().interrupt();
    }
  }
}
