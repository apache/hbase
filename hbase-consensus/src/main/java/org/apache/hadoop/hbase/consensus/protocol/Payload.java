package org.apache.hadoop.hbase.consensus.protocol;

import com.google.common.util.concurrent.SettableFuture;
import java.nio.ByteBuffer;

/**
 * Wrap the data to commit into a object along with the result.
 */
public class Payload {
  final ByteBuffer entries;
  final SettableFuture<Long> result;

  public Payload(ByteBuffer entries, SettableFuture<Long> result) {
    this.entries = entries;
    this.result = result;
  }

  public ByteBuffer getEntries() {
    return entries;
  }

  public SettableFuture<Long> getResult() {
    return result;
  }
}
