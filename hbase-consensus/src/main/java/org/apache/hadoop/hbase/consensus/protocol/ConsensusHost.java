package org.apache.hadoop.hbase.consensus.protocol;

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
