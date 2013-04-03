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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;


/**
 * Implementation for hdfs
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSHDFSUtils extends FSUtils{
  private static final Log LOG = LogFactory.getLog(FSHDFSUtils.class);

  /**
   * Recover the lease from HDFS, retrying multiple times.
   */
  @Override
  public void recoverFileLease(final FileSystem fs, final Path p, Configuration conf)
      throws IOException {
    // lease recovery not needed for local file system case.
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;

    LOG.info("Recovering file " + p);
    long startWaiting = EnvironmentEdgeManager.currentTimeMillis();
    // Default is 15 minutes. It's huge, but the idea is that if we have a major issue, HDFS
    //  usually needs 10 minutes before marking the nodes as dead. So we're putting ourselves
    //  beyond that limit 'to be safe'.
    long recoveryTimeout = conf.getInt("hbase.lease.recovery.timeout", 900000) + startWaiting;
    boolean recovered = false;
    int nbAttempt = 0;
    while (!recovered) {
      nbAttempt++;
      try {
        // recoverLease is asynchronous. We expect it to return true at the first call if the
        //  file is closed. So, it returns:
        //    - false when it starts the lease recovery (i.e. lease recovery not *yet* done
        //    - true when the lease recovery has succeeded or the file is closed.
        recovered = dfs.recoverLease(p);
        LOG.info("Attempt " + nbAttempt + " to recoverLease on file " + p +
            " returned " + recovered + ", trying for " +
            (EnvironmentEdgeManager.currentTimeMillis() - startWaiting) + "ms");
      } catch (IOException e) {
        if (e instanceof LeaseExpiredException && e.getMessage().contains("File does not exist")) {
          // This exception comes out instead of FNFE, fix it
          throw new FileNotFoundException("The given HLog wasn't found at " + p);
        }
        LOG.warn("Got IOException on attempt " + nbAttempt + " to recover lease for file " + p +
            ", retrying.", e);
      }
      if (!recovered) {
        // try at least twice.
        if (nbAttempt > 2 && recoveryTimeout < EnvironmentEdgeManager.currentTimeMillis()) {
          LOG.error("Can't recoverLease after " + nbAttempt + " attempts and " +
              (EnvironmentEdgeManager.currentTimeMillis() - startWaiting) + "ms " + " for " + p +
              " - continuing without the lease, but we could have a data loss.");
        } else {
          try {
            Thread.sleep(nbAttempt < 3 ? 500 : 1000);
          } catch (InterruptedException ie) {
            InterruptedIOException iioe = new InterruptedIOException();
            iioe.initCause(ie);
            throw iioe;
          }
        }
      }
    }
  }
}
