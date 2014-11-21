package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.consensus.protocol.Payload;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface RaftEventListener {
  ByteBuffer becameLeader() throws IOException;
  void becameNonLeader();
  void commit(final long index, final Payload payload);
  boolean canStepDown();
  long getMinUnpersistedIndex();
  DataStoreState getState();
  void updatePeerAvailabilityStatus(String peerAddress, boolean isAvailable);
  void closeDataStore();
}
