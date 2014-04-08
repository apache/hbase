/**
 * Copyright 2007 The Apache Software Foundation
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

/**
 * Contains the HRegionInfo for the region and the HServerAddress for the
 * HRegionServer serving the region
 */
@ThriftStruct
public class HRegionLocation implements Comparable<HRegionLocation> {
  private HRegionInfo regionInfo;
  private HServerAddress serverAddress;
  private long serverStartCode;

  /**
   * Constructor
   *
   * @param regionInfo the HRegionInfo for the region
   * @param serverAddress the HServerAddress for the region server
   */
  public HRegionLocation(HRegionInfo regionInfo, HServerAddress serverAddress) {
    this(regionInfo, serverAddress, -1);
  }

  @ThriftConstructor
  public HRegionLocation(
    @ThriftField(1) HRegionInfo regionInfo,
    @ThriftField(2) HServerAddress serverAddress,
    @ThriftField(3) long serverStartCode) {
    this.regionInfo = regionInfo;
    this.serverAddress = serverAddress;
    this.serverStartCode = serverStartCode;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "address: " + this.serverAddress.toString() + ", serverStartCode: " +
      this.serverStartCode + ", regioninfo: " + this.regionInfo;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionLocation)) {
      return false;
    }
    return this.compareTo((HRegionLocation)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.regionInfo.hashCode();
    result ^= this.serverAddress.hashCode();
    result ^= (int)this.serverStartCode;
    return result;
  }

  /** @return HRegionInfo */
  @ThriftField(1)
  public HRegionInfo getRegionInfo(){
    return regionInfo;
  }

  /** @return HServerAddress */
  @ThriftField(2)
  public HServerAddress getServerAddress(){
    return serverAddress;
  }

  @ThriftField(3)
  public long getServerStartCode() {
    return serverStartCode;
  }

  //
  // Comparable
  //

  public int compareTo(HRegionLocation o) {
    int result = this.regionInfo.compareTo(o.regionInfo);
    if(result == 0) {
      result = this.serverAddress.compareTo(o.serverAddress);
    }
    if(result == 0) {
      result = (this.serverStartCode > o.serverStartCode) ? 1 :
      (this.serverStartCode < o.serverStartCode)? -1 :  0;
    }
    return result;
  }
}
