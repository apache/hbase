package org.apache.hadoop.hbase.consensus.protocol;

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
//import org.apache.http.annotation.Immutable;

@ThriftStruct
//@Immutable
public final class ConsensusHost {
  final long term;
  final String hostId;

  @ThriftConstructor
  public ConsensusHost(
    @ThriftField(1) final long term,
    @ThriftField(2) String address) {
    this.term = term;
    this.hostId = address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ConsensusHost)) {
      return false;
    }

    ConsensusHost that = (ConsensusHost) o;

    if (term != that.term || !hostId.equals(that.hostId)) {
      return false;
    }

    return true;
  }

  @ThriftField(1)
  public long getTerm() {
    return term;
  }

  @ThriftField(2)
  public String getHostId() {
    return hostId;
  }

  @Override
  public String toString() {
    return "{host=" + hostId + ", term=" + term + "}";
  }
}
