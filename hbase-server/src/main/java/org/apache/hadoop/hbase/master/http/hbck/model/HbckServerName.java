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
package org.apache.hadoop.hbase.master.http.hbck.model;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HbckServerName {
  private final String hostName;
  private final int hostPort;
  private final long startCode;

  public HbckServerName(String hostName, int hostPort, long startCode) {
    this.hostName = hostName;
    this.hostPort = hostPort;
    this.startCode = startCode;
  }

  public String getHostName() {
    return hostName;
  }

  public int getHostPort() {
    return hostPort;
  }

  public long getStartCode() {
    return startCode;
  }
}
