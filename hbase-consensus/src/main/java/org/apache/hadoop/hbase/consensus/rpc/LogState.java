package org.apache.hadoop.hbase.consensus.rpc;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.log.SeedLogFile;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ThriftStruct
public final class LogState {
  private List<LogFileInfo> committedLogFiles;
  private List<LogFileInfo> uncommittedLogFiles;
  private String peerState;
  private EditId lastCommittedEdit;

  // Used to pass error message. If not null, other fields are considered
  // invalid and should be discarded.
  private String errMsg;

  public LogState(String errMsg) {
    this.errMsg = errMsg;
    this.peerState = null;
    lastCommittedEdit = null;

    if (this.errMsg == null) {
      this.committedLogFiles = new ArrayList<>();
      this.uncommittedLogFiles = new ArrayList<>();
    }
  }

  @ThriftConstructor
  public LogState(
      @ThriftField(1) List<LogFileInfo> committedLogFiles,
      @ThriftField(2) List<LogFileInfo> uncommittedLogFiles,
      @ThriftField(3) String peerState,
      @ThriftField(4) String errMsg,
      @ThriftField(5) EditId lastCommittedEdit) {
    this.committedLogFiles = committedLogFiles;
    this.uncommittedLogFiles = uncommittedLogFiles;
    this.peerState = peerState;
    this.errMsg = errMsg;
    this.lastCommittedEdit = lastCommittedEdit;
  }

  @ThriftField(1)
  public List<LogFileInfo> getCommittedLogFiles() {
    return this.committedLogFiles;
  }

  @ThriftField(2)
  public List<LogFileInfo> getUncommittedLogFiles() {
    return this.uncommittedLogFiles;
  }

  @ThriftField(3)
  public String getPeerState() {
    return this.peerState;
  }

  @ThriftField(4)
  public String getErrMsg() {
    return this.errMsg;
  }

  @ThriftField(5)
  public EditId getLastCommittedEdit() {
    return this.lastCommittedEdit;
  }

  public void setPeerState(String peerState) {
    this.peerState = peerState;
  }

  public void addCommittedLogFile(LogFileInfo info) {
    this.committedLogFiles.add(info);
  }

  public void addUncommittedLogFile(LogFileInfo info) {
    this.uncommittedLogFiles.add(info);
  }

  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

  public void setLastCommittedEdit(EditId lastCommittedEdit) {
    this.lastCommittedEdit = lastCommittedEdit;
  }

  public boolean isErrorState() {
    return errMsg != null;
  }

  public void sortLogFiles() {
    if (committedLogFiles != null && !committedLogFiles.isEmpty()) {
      Collections.sort(committedLogFiles);
    }
    if (uncommittedLogFiles != null && !uncommittedLogFiles.isEmpty()) {
      Collections.sort(uncommittedLogFiles);
    }
  }

  private Pair<Long, Long> getIndexRange(List<LogFileInfo> logFiles) {
    if (logFiles == null || logFiles.isEmpty()) {
      return null;
    }
    Long startIndex = logFiles.get(0).getInitialIndex();
    Long lastIndex = logFiles.get(logFiles.size() - 1).getLastIndex();
    return new Pair<>(startIndex, lastIndex);
  }

  @Override
  public String toString() {
    if (errMsg != null) {
      return errMsg;
    }

    sortLogFiles();

    StringBuilder sb = new StringBuilder();
    sb.append("{ ");

    Pair<Long, Long> uncommittedRange = getIndexRange(uncommittedLogFiles);
    if (uncommittedRange != null) {
      sb.append("Uncommitted [").append(uncommittedRange.getFirst())
          .append(", ").append(uncommittedRange.getSecond()).append("] ");
    }

    Pair<Long, Long> committedRange = getIndexRange(committedLogFiles);
    if (committedRange != null) {
      sb.append("Committed [").append(committedRange.getFirst())
          .append(", ").append(committedRange.getSecond()).append("] ");
    }
    for (LogFileInfo info : committedLogFiles) {
      if (SeedLogFile.isSeedFile(info.getFilename())) {
        sb.append("Seed File [").append(info.getInitialIndex()).append(", ")
            .append(info.getLastIndex()).append("]");
      }
    }
    sb.append(" } ; {Peers: ");
    sb.append(peerState);
    sb.append(" }");
    return sb.toString();
  }
}
