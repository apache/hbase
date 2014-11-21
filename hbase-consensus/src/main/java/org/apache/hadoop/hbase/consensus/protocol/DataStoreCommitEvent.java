package org.apache.hadoop.hbase.consensus.protocol;

import com.lmax.disruptor.EventFactory;

/**
 * Class which contains information about the transaction to commit. This will
 * be used by the Disruptor Producer.
 */
public class DataStoreCommitEvent {

  private Payload value;
  private long commitIndex;

  public DataStoreCommitEvent() {
    this.value = null;
  }

  public Payload getValue() {
    return value;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setValue(long commitIndex, Payload value) {
    this.commitIndex = commitIndex;
    this.value = value;
  }

  public final static EventFactory<DataStoreCommitEvent> EVENT_FACTORY =
    new EventFactory<DataStoreCommitEvent>() {
    public DataStoreCommitEvent newInstance() {
      return new DataStoreCommitEvent();
    }
  };
}
