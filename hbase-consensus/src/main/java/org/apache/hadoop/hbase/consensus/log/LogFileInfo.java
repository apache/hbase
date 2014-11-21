/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.consensus.log;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.consensus.protocol.EditId;

import java.io.File;

@ThriftStruct
public final class LogFileInfo implements Comparable<LogFileInfo> {
  private final EditId firstEditId;
  private final EditId lastEditId;
  private final long nbytes;
  private final String path;
  private byte[] md5sum = null;
  private long lastVerifiedModificationTime = 0L;
  private final long creationTime;

  @ThriftConstructor
  public LogFileInfo(
      @ThriftField(1) String path,
      @ThriftField(2) long nbytes,
      @ThriftField(3) EditId start,
      @ThriftField(4) EditId last,
      @ThriftField(5) long lastVerifiedModificationTime,
      @ThriftField(6) long creationTime) {
    this.path = path;
    this.nbytes = nbytes;
    this.firstEditId = start;
    this.lastEditId = last;
    this.lastVerifiedModificationTime = lastVerifiedModificationTime;
    this.creationTime = creationTime;
  }

  @ThriftField(1)
  public String getAbsolutePath() {
    return path;
  }

  @ThriftField(2)
  public long length() {
    return nbytes;
  }

  @ThriftField(3)
  public EditId getFirstEditId() {
    return firstEditId;
  }

  @ThriftField(4)
  public EditId getLastEditId() {
    return lastEditId;
  }

  @ThriftField(5)
  public long getLastVerifiedModificationTime() {
    return lastVerifiedModificationTime;
  }

  @ThriftField(6)
  public long getCreationTime() {
    return creationTime;
  }

  public void setMD5(byte[] md5) {
    this.md5sum = md5;
  }

  public byte[] getMD5() {
    return md5sum;
  }

  public void setLastVerifiedModificationTime(long modtime) {
    this.lastVerifiedModificationTime = modtime;
  }

  public long getTxnCount() {
    return getLastIndex() + 1L - getInitialIndex();
  }

  public String getFilename() {
    return (new File(getAbsolutePath())).getName();
  }

  public long getInitialIndex() {
    return getFirstEditId().getIndex();
  }

  public long getLastIndex() {
    return getLastEditId().getIndex();
  }

  public boolean validate() {
    return getFirstEditId() != null && getLastEditId() != null
      && getFirstEditId().getTerm() <= getLastEditId().getTerm()
      && getInitialIndex() <= getLastIndex()
      && getAbsolutePath() != null
      && getFilename() != null
      && length() > 0;
  }

  public String toString() {
    return "LogFileInfo[" + getFirstEditId() + " .. " + getLastEditId()
      + ", " + length() + " bytes, " + getAbsolutePath() + ", "
      + ", @ " + getLastVerifiedModificationTime() + ", createdAt "
      + getCreationTime() + "]";
  }

  @Override
  public int compareTo(LogFileInfo that) {
    int ret = Long.valueOf(getInitialIndex()).compareTo(that.getInitialIndex());
    if (ret != 0) {
      return ret;
    } else {
      return Long.valueOf(getLastIndex()).compareTo(that.getLastIndex());
    }
  }
}
