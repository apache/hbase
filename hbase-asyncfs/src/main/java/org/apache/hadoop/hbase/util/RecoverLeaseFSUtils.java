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
package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for recovering file lease for hdfs.
 */
@InterfaceAudience.Private
public final class RecoverLeaseFSUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RecoverLeaseFSUtils.class);

  private RecoverLeaseFSUtils() {
  }

  public static void recoverFileLease(FileSystem fs, Path p, Configuration conf)
    throws IOException {
    recoverFileLease(fs, p, conf, null);
  }

  /**
   * Recover the lease from HDFS, retrying multiple times.
   */
  public static void recoverFileLease(FileSystem fs, Path p, Configuration conf,
    CancelableProgressable reporter) throws IOException {
    if (fs instanceof FilterFileSystem) {
      fs = ((FilterFileSystem) fs).getRawFileSystem();
    }
    // lease recovery not needed for local file system case.
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    recoverDFSFileLease((DistributedFileSystem) fs, p, conf, reporter);
  }

  /*
   * Run the dfs recover lease. recoverLease is asynchronous. It returns: -false when it starts the
   * lease recovery (i.e. lease recovery not *yet* done) - true when the lease recovery has
   * succeeded or the file is closed. But, we have to be careful. Each time we call recoverLease, it
   * starts the recover lease process over from the beginning. We could put ourselves in a situation
   * where we are doing nothing but starting a recovery, interrupting it to start again, and so on.
   * The findings over in HBASE-8354 have it that the namenode will try to recover the lease on the
   * file's primary node. If all is well, it should return near immediately. But, as is common, it
   * is the very primary node that has crashed and so the namenode will be stuck waiting on a socket
   * timeout before it will ask another datanode to start the recovery. It does not help if we call
   * recoverLease in the meantime and in particular, subsequent to the socket timeout, a
   * recoverLease invocation will cause us to start over from square one (possibly waiting on socket
   * timeout against primary node). So, in the below, we do the following: 1. Call recoverLease. 2.
   * If it returns true, break. 3. If it returns false, wait a few seconds and then call it again.
   * 4. If it returns true, break. 5. If it returns false, wait for what we think the datanode
   * socket timeout is (configurable) and then try again. 6. If it returns true, break. 7. If it
   * returns false, repeat starting at step 5. above. If HDFS-4525 is available, call it every
   * second and we might be able to exit early.
   */
  private static boolean recoverDFSFileLease(final DistributedFileSystem dfs, final Path p,
    final Configuration conf, final CancelableProgressable reporter) throws IOException {
    LOG.info("Recover lease on dfs file " + p);
    long startWaiting = EnvironmentEdgeManager.currentTime();
    // Default is 15 minutes. It's huge, but the idea is that if we have a major issue, HDFS
    // usually needs 10 minutes before marking the nodes as dead. So we're putting ourselves
    // beyond that limit 'to be safe'.
    long recoveryTimeout = conf.getInt("hbase.lease.recovery.timeout", 900000) + startWaiting;
    // This setting should be a little bit above what the cluster dfs heartbeat is set to.
    long firstPause = conf.getInt("hbase.lease.recovery.first.pause", 4000);
    // This should be set to how long it'll take for us to timeout against primary datanode if it
    // is dead. We set it to 64 seconds, 4 second than the default READ_TIMEOUT in HDFS, the
    // default value for DFS_CLIENT_SOCKET_TIMEOUT_KEY. If recovery is still failing after this
    // timeout, then further recovery will take liner backoff with this base, to avoid endless
    // preemptions when this value is not properly configured.
    long subsequentPauseBase = conf.getLong("hbase.lease.recovery.dfs.timeout", 64 * 1000);

    Method isFileClosedMeth = null;
    // whether we need to look for isFileClosed method
    boolean findIsFileClosedMeth = true;
    boolean recovered = false;
    // We break the loop if we succeed the lease recovery, timeout, or we throw an exception.
    for (int nbAttempt = 0; !recovered; nbAttempt++) {
      recovered = recoverLease(dfs, nbAttempt, p, startWaiting);
      if (recovered) {
        break;
      }
      checkIfCancelled(reporter);
      if (checkIfTimedout(conf, recoveryTimeout, nbAttempt, p, startWaiting)) {
        break;
      }
      try {
        // On the first time through wait the short 'firstPause'.
        if (nbAttempt == 0) {
          Thread.sleep(firstPause);
        } else {
          // Cycle here until (subsequentPause * nbAttempt) elapses. While spinning, check
          // isFileClosed if available (should be in hadoop 2.0.5... not in hadoop 1 though.
          long localStartWaiting = EnvironmentEdgeManager.currentTime();
          while ((EnvironmentEdgeManager.currentTime() - localStartWaiting) < subsequentPauseBase *
            nbAttempt) {
            Thread.sleep(conf.getInt("hbase.lease.recovery.pause", 1000));
            if (findIsFileClosedMeth) {
              try {
                isFileClosedMeth =
                  dfs.getClass().getMethod("isFileClosed", new Class[] { Path.class });
              } catch (NoSuchMethodException nsme) {
                LOG.debug("isFileClosed not available");
              } finally {
                findIsFileClosedMeth = false;
              }
            }
            if (isFileClosedMeth != null && isFileClosed(dfs, isFileClosedMeth, p)) {
              recovered = true;
              break;
            }
            checkIfCancelled(reporter);
          }
        }
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
    return recovered;
  }

  private static boolean checkIfTimedout(final Configuration conf, final long recoveryTimeout,
    final int nbAttempt, final Path p, final long startWaiting) {
    if (recoveryTimeout < EnvironmentEdgeManager.currentTime()) {
      LOG.warn("Cannot recoverLease after trying for " +
        conf.getInt("hbase.lease.recovery.timeout", 900000) +
        "ms (hbase.lease.recovery.timeout); continuing, but may be DATALOSS!!!; " +
        getLogMessageDetail(nbAttempt, p, startWaiting));
      return true;
    }
    return false;
  }

  /**
   * Try to recover the lease.
   * @return True if dfs#recoverLease came by true.
   */
  private static boolean recoverLease(final DistributedFileSystem dfs, final int nbAttempt,
    final Path p, final long startWaiting) throws FileNotFoundException {
    boolean recovered = false;
    try {
      recovered = dfs.recoverLease(p);
      LOG.info((recovered ? "Recovered lease, " : "Failed to recover lease, ") +
        getLogMessageDetail(nbAttempt, p, startWaiting));
    } catch (IOException e) {
      if (e instanceof LeaseExpiredException && e.getMessage().contains("File does not exist")) {
        // This exception comes out instead of FNFE, fix it
        throw new FileNotFoundException("The given WAL wasn't found at " + p);
      } else if (e instanceof FileNotFoundException) {
        throw (FileNotFoundException) e;
      }
      LOG.warn(getLogMessageDetail(nbAttempt, p, startWaiting), e);
    }
    return recovered;
  }

  /**
   * @return Detail to append to any log message around lease recovering.
   */
  private static String getLogMessageDetail(final int nbAttempt, final Path p,
    final long startWaiting) {
    return "attempt=" + nbAttempt + " on file=" + p + " after " +
      (EnvironmentEdgeManager.currentTime() - startWaiting) + "ms";
  }

  /**
   * Call HDFS-4525 isFileClosed if it is available.
   * @return True if file is closed.
   */
  private static boolean isFileClosed(final DistributedFileSystem dfs, final Method m,
    final Path p) {
    try {
      return (Boolean) m.invoke(dfs, p);
    } catch (SecurityException e) {
      LOG.warn("No access", e);
    } catch (Exception e) {
      LOG.warn("Failed invocation for " + p.toString(), e);
    }
    return false;
  }

  private static void checkIfCancelled(final CancelableProgressable reporter)
    throws InterruptedIOException {
    if (reporter == null) {
      return;
    }
    if (!reporter.progress()) {
      throw new InterruptedIOException("Operation cancelled");
    }
  }
}
