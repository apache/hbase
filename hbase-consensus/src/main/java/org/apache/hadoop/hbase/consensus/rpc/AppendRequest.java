package org.apache.hadoop.hbase.consensus.rpc;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

@ThriftStruct
public final class AppendRequest extends Request<AppendResponse> {

  private final String regionId;

  private final ConsensusHost leaderId;

  private final boolean isHeartBeat;

  private final long commitIndex;

  private final long persistedIndex;

  private final EditId prevLogId;

  private final List<EditId> logIds;

  private final List<ByteBuffer> listOfEdits;

  private boolean isTraceable = false;

  private SettableFuture<AppendResponse> response;

  @ThriftConstructor
  public AppendRequest(
      @ThriftField(1) final String regionId,
      @ThriftField(2) final ConsensusHost id,
      @ThriftField(3) final boolean isHeartBeat,
      @ThriftField(4) final long commitIndex,
      @ThriftField(5) final long persistedIndex,
      @ThriftField(6) final EditId prevLogId,
      @ThriftField(7) final List<EditId> logIds,
      @ThriftField(8) final List<ByteBuffer> listOfEdits
  ) {
    this.regionId = regionId;
    this.leaderId = id;
    this.isHeartBeat = isHeartBeat;
    this.commitIndex = commitIndex;
    this.persistedIndex = persistedIndex;
    this.prevLogId = prevLogId;
    this.logIds = logIds;
    this.listOfEdits = listOfEdits;
    assert logIds.size() == listOfEdits.size();
  }

  public AppendRequest(final AppendRequest r) {
    this.regionId = r.regionId;
    this.leaderId = r.leaderId;
    this.isHeartBeat = r.isHeartBeat;
    this.commitIndex = r.commitIndex;
    this.persistedIndex = r.persistedIndex;
    this.prevLogId = r.prevLogId;
    this.logIds = r.logIds;
    this.listOfEdits = r.listOfEdits;
  }

  @ThriftField(1)
  public String getRegionId() {
    return regionId;
  }

  @ThriftField(2)
  public ConsensusHost getLeaderId() {
    return leaderId;
  }

  @ThriftField(3)
  public boolean isHeartBeat() {
    return this.isHeartBeat;
  }

  @ThriftField(4)
  public long getCommitIndex() {
    return commitIndex;
  }

  @ThriftField(5)
  public long getPersistedIndex() {
    return persistedIndex;
  }

  @ThriftField(6)
  public EditId getPrevLogId() {
    return prevLogId;
  }

  @ThriftField(7)
  public List<EditId> getLogIds() {
    return logIds;
  }

  @ThriftField(8)
  public List<ByteBuffer> getListOfEdits() {
    return listOfEdits;
  }

  public EditId getLogId(int index) {
    return logIds.get(index);
  }

  public ByteBuffer getEdit(int index) {
    return listOfEdits.get(index);
  }

  public void createAppendResponse() {
    if (response == null) {
      response = SettableFuture.create();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb = sb.append("AppendRequest{")
      .append("region = ").append(regionId)
      .append(", address = ").append(leaderId)
      .append(", heartbeat = ").append(isHeartBeat)
      .append(", commit index = ").append(commitIndex)
      .append(", persisted index = ").append(persistedIndex)
      .append(", prev log edit = ").append(prevLogId);

    sb.append(", current edit logs = ")
      .append(RaftUtil.listToString(logIds));

    if (listOfEdits != null) {
      sb.append(", edit sizes = [");

      for (int i=0; i < listOfEdits.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        if (listOfEdits.get(i) != null) {
          sb.append("" + listOfEdits.get(i));
        }
      }
      sb.append("] ");
    }

    sb.append('}');
    return sb.toString();
  }

  public ListenableFuture<AppendResponse> getResponse() {
    return response;
  }

  public void setResponse(AppendResponse r) {
    if (response != null) {
      response.set(r);
    }
  }

  public void setError(final Throwable exception) {
    if (response != null) {
      response.setException(exception);
    }
  }

  public boolean validateFields() {
    assert getLogIds() != null;
    assert getListOfEdits() != null;
    assert getLogIds().size() == getListOfEdits().size();
    return true;
  }

  public int logCount() {
    assert validateFields();
    return getLogIds().size();
  }

  public static AppendRequest createSingleAppendRequest(
    final String regionId,
    final ConsensusHost id,
    final EditId logId,
    final EditId prevLogId,
    final long commitIndex,
    final long persistedIndex,
    final boolean isHeartBeat,
    final ByteBuffer edits) {
    return new AppendRequest(
      regionId,
      id,
      isHeartBeat,
      commitIndex,
      persistedIndex,
      prevLogId,
      Arrays.asList(logId),
      Arrays.asList(edits));
  }

  public void enableTraceable() {
    this.isTraceable = true;
  }

  public void disableTraceable() {
    this.isTraceable = false;
  }

  public boolean isTraceable() {
    return this.isTraceable;
  }
}
