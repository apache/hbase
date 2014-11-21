package org.apache.hadoop.hbase.consensus.server.peer;

import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;

public interface PeerServerImmutableContext {
  EditId getLastEditID();
  AppendRequest getLatestRequest();
  int getRank();
}
