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
package org.apache.hadoop.hbase;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class RootRegionLocation {
  private String hostname;
  private int port;
  private long startcode;
  private ProtocolVersion protocolVersion;

  @ThriftConstructor
  public RootRegionLocation(@ThriftField(1) String hostname,
      @ThriftField(2) int port,
      @ThriftField(3) long startcode,
      @ThriftField(4) ProtocolVersion protocolVersion) {
    this.hostname = hostname;
    this.port = port;
    this.startcode = startcode;
    this.protocolVersion = protocolVersion;
  }

  @ThriftField(1)
  public String getHostName() {
    return hostname;
  }

  @ThriftField(2)
  public int getPort() {
    return port;
  }

  @ThriftField(3)
  public long getStartCode() {
    return startcode;
  }

  @ThriftField(4)
  public ProtocolVersion getProtocolVersion() {
    return this.protocolVersion;
  }

  @Override
  public String toString() {
    return hostname + ":" + port +
        ":" + startcode + ":" + protocolVersion.name();
  }

  public HServerAddress getServerAddress() {
    return new HServerAddress(hostname, port);
  }
}
