package org.apache.hadoop.hbase.regionserver;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftEnum;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public final class DataStoreState {

  @ThriftEnum
  public static enum RegionMode {
    UNASSIGNED,
    ACTIVE,  // does memstore updates, and flushing/compaction.
    WITNESS, // does memstore updates. No flushing/compaction.
    // Prunes the memstore whenever the active replica flushes.
    SHADOW_WITNESS; // does not update memstore. No flushing/compaction.

    boolean isWitness() {
      return this.equals(WITNESS);
    }

    boolean isActive() {
      return this.equals(ACTIVE);
    }

    boolean isShadowWitness() {
      return this.equals(SHADOW_WITNESS);
    }

    boolean isUnassigned() {
      return this.equals(UNASSIGNED);
    }
  };

  private final String dataStoreId;
  private volatile long committedUpto;
  private volatile long canCommitUpto;
  private volatile RegionMode mode;

  public DataStoreState(final String dataStoreId) {
    this.dataStoreId = dataStoreId;
    committedUpto = -1;
    canCommitUpto = -1;
    mode = RegionMode.UNASSIGNED;
  }

  @ThriftConstructor
  public DataStoreState(@ThriftField(1) String dataStoreId,
                        @ThriftField(2) long committedUpto,
                        @ThriftField(3) long canCommitUpto,
                        @ThriftField(4) RegionMode mode) {
    this.dataStoreId = dataStoreId;
    this.committedUpto = committedUpto;
    this.canCommitUpto = canCommitUpto;
    this.mode = mode;
  }

  @ThriftField(1)
  public String getDataStoreId() {
    return dataStoreId;
  }

  @ThriftField(2)
  public long getCommittedUpto() {
    return committedUpto;
  }

  @ThriftField(3)
  public long getCanCommitUpto() {
    return canCommitUpto;
  }

  @ThriftField(4)
  public RegionMode getMode() {
    return mode;
  }

  public void setCommittedUpto(long committedUpto) {
    this.committedUpto = committedUpto;
    this.canCommitUpto = committedUpto;
  }

  public void setMode(RegionMode mode) {
    this.mode = mode;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("{");
    builder.append("id=" + dataStoreId + ", ");
    builder.append("mode=" + mode + ", ");
    builder.append("canCommitUpto=" + canCommitUpto + ", ");
    builder.append("committedUptoIndex=" + committedUpto);
    builder.append("}");

    return builder.toString();
  }
}
