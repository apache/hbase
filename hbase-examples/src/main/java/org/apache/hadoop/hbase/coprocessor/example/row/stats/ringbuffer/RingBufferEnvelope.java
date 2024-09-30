package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

@InterfaceAudience.Private
public final class RingBufferEnvelope {

  private RingBufferPayload payload;

  public void load(RingBufferPayload payload) {
    this.payload = payload;
  }

  public RingBufferPayload getPayload() {
    final RingBufferPayload payload = this.payload;
    this.payload = null;
    return payload;
  }
}
