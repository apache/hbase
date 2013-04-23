/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.mortbay.log.Log;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.SyncFailedException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exception thrown by HTable methods when an attempt to do something (like
 * commit changes) fails after a bunch of retries.
 */
public class RetriesExhaustedException extends IOException {
  private static final long serialVersionUID = 1876775844L;
  private Map<String, HRegionFailureInfo> failureInfo = null;

  /**
   * Create a new RetriesExhaustedException from the list of prior failures.
   * @param serverName name of HRegionServer
   * @param regionName name of region
   * @param row The row we were pursuing when we ran out of retries
   * @param numTries The number of tries we made
   * @param exceptions List of exceptions that failed before giving up
   */
  public RetriesExhaustedException(String serverName, final byte [] regionName,
      final byte []  row, int numTries, List<Throwable> exceptions) {
    super(getMessage(serverName, regionName, row, numTries, exceptions));
    failureInfo = new HashMap<String, HRegionFailureInfo>();
    String regName = Bytes.toStringBinary(regionName);

    if (!failureInfo.containsKey(regName)) {
      failureInfo.put(regName, new HRegionFailureInfo(regName));
    }

    failureInfo.get(regName).setServerName(serverName);
    this.failureInfo.get(regName).addAllExceptions(exceptions);
  }

  public RetriesExhaustedException(final Map<String, HRegionFailureInfo> failureInfo,
      String msg) {
    super(msg);
    this.failureInfo = failureInfo;
  }

  private static String getMessage(String serverName, final byte [] regionName,
      final byte [] row,
      int numTries, List<Throwable> exceptions) {
    StringBuilder buffer = new StringBuilder("Trying to contact region server ");
    buffer.append(serverName);
    buffer.append(" for region ");
    buffer.append(regionName == null? "": Bytes.toStringBinary(regionName));
    buffer.append(", row '");
    buffer.append(row == null? "": Bytes.toStringBinary(row));
    buffer.append("', but failed after ");
    buffer.append(numTries + 1);
    buffer.append(" attempts.\nExceptions:\n");
    for (Throwable t : exceptions) {
      buffer.append(t.toString());
      
      StringWriter errors = new StringWriter();
      t.printStackTrace(new PrintWriter(errors));
      buffer.append(errors.toString());
      buffer.append("\n");
      
      try { 
        errors.close();
      } catch (IOException e) {} // ignore
    }
    return buffer.toString();
  }

  public Set<String> getRegionNames() {
    return this.failureInfo.keySet();
  }

  public HRegionFailureInfo getFailureInfoForRegion(String regionName) {
    return this.failureInfo.get(regionName);
  }

  public Map<String, HRegionFailureInfo> getFailureInfo() {
    return this.failureInfo;
  }

  /**
   * Tells whether the operation was attempted by region server or not.
   * @return
   */
  public boolean wasOperationAttemptedByServer() {

    // Let's iterate through all the exceptions and if any one of them
    // was thrown by a RegionServer, then the operation might have
    // been tried and hence the clients should take appropriate steps
    // to validate their cache.
    for (HRegionFailureInfo failures : failureInfo.values()) {
      for (Throwable e : failures.getExceptions()) {
        if (serverOperationExecutionException(e)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Tells whether it is safe or not to consider that the current exception
   * was thrown by the region server while executing the actual operation.
   * @param e
   * @return
   */
  private boolean serverOperationExecutionException(final Throwable e) {

    // In the following scenarios it is safe to assume that the operation was not
    // performed by the region server
    // a) ConnectException : Client was not able to connect to the region server
    // b) NotServingRegionException : the region is not opened on the server.
    // c) NoServerForRegionException : there is no server present for this region.
    //
    // Apart from the above 3 exceptions, we are not sure if the operation was
    // attempted on region server or not. Being conservative, we return true for
    // all the other type of exceptions.
    return !(e instanceof ConnectException ||
        e instanceof NotServingRegionException ||
        e instanceof NoServerForRegionException);
  }
}
