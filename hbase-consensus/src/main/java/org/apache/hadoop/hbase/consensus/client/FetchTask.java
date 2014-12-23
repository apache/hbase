package org.apache.hadoop.hbase.consensus.client;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Fetch log files from a peer
 */
@ThriftStruct
public final class FetchTask implements Runnable {
  // The peer's address
  private final String addr;

  // List of files that are going to be downloaded from the peer
  private List<LogFileInfo> fileInfos = new ArrayList<>();

  public FetchTask(String address) {
    addr = address;
  }

  @ThriftConstructor
  public FetchTask(
      @ThriftField(1) String address,
      @ThriftField(2) List<LogFileInfo> infos) {
    addr = address;
    fileInfos = infos;
  }

  @ThriftField(1)
  public String getAddr() {
    return addr;
  }

  @ThriftField(2)
  public List<LogFileInfo> getFileInfos() {
    return fileInfos;
  }

  public void addTask(LogFileInfo info) {
    fileInfos.add(info);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Requests for peer ").append(addr).append(" :");
    for (LogFileInfo info : fileInfos) {
      sb.append(" {").append(info.getFilename()).append(",[")
          .append(info.getInitialIndex()).append(",")
          .append(info.getLastIndex()).append("]}");
    }

    return sb.toString();
  }

  @Override
  public void run() {
    // TODO in part 2: fetch log files from the peer!
  }
}
