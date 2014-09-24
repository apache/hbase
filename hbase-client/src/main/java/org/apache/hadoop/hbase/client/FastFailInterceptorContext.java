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
package org.apache.hadoop.hbase.client;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
class FastFailInterceptorContext extends
    RetryingCallerInterceptorContext {

  // The variable that indicates whether we were able to connect with the server
  // in the last run
  private MutableBoolean couldNotCommunicateWithServer = new MutableBoolean(
      false);

  // The variable which indicates whether this was a retry or the first time
  private boolean didTry = false;

  // The failure info that is associated with the machine which we are trying to
  // contact as part of this attempt.
  private FailureInfo fInfo = null;

  // Variable indicating that the thread that is currently executing the
  // operation is in a mode where it would retry instead of failing fast, so
  // that we can figure out whether making contact with the server is
  // possible or not.
  private boolean retryDespiteFastFailMode = false;

  // The server that would be contacted to successfully complete this operation.
  private ServerName server;

  // The number of the retry we are currenty doing.
  private int tries;

  public MutableBoolean getCouldNotCommunicateWithServer() {
    return couldNotCommunicateWithServer;
  }

  public FailureInfo getFailureInfo() {
    return fInfo;
  }

  public ServerName getServer() {
    return server;
  }

  public int getTries() {
    return tries;
  }

  public boolean didTry() {
    return didTry;
  }

  public boolean isRetryDespiteFastFailMode() {
    return retryDespiteFastFailMode;
  }

  public void setCouldNotCommunicateWithServer(
      MutableBoolean couldNotCommunicateWithServer) {
    this.couldNotCommunicateWithServer = couldNotCommunicateWithServer;
  }

  public void setDidTry(boolean didTry) {
    this.didTry = didTry;
  }

  public void setFailureInfo(FailureInfo fInfo) {
    this.fInfo = fInfo;
  }

  public void setRetryDespiteFastFailMode(boolean retryDespiteFastFailMode) {
    this.retryDespiteFastFailMode = retryDespiteFastFailMode;
  }

  public void setServer(ServerName server) {
    this.server = server;
  }

  public void setTries(int tries) {
    this.tries = tries;
  }

  public void clear() {
    server = null;
    fInfo = null;
    didTry = false;
    couldNotCommunicateWithServer.setValue(false);
    retryDespiteFastFailMode = false;
    tries = 0;
  }

  public FastFailInterceptorContext prepare(RetryingCallable<?> callable) {
    return prepare(callable, 0);
  }

  public FastFailInterceptorContext prepare(RetryingCallable<?> callable,
      int tries) {
    if (callable instanceof RegionServerCallable) {
      RegionServerCallable<?> retryingCallable = (RegionServerCallable<?>) callable;
      server = retryingCallable.getLocation().getServerName();
    }
    this.tries = tries;
    return this;
  }
}
