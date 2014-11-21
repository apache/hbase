package org.apache.hadoop.hbase.consensus.client;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Fetch log files from a peer
 */
@ThriftStruct
public final class FetchTask implements Runnable {
  // The peer's address
  private final String addr;

  // List of files that are going to be downloaded from the peer
  private List<LogFileInfo> fileInfos = new ArrayList<>();

  public FetchTask(String address) {
    addr = address;
  }

  @ThriftConstructor
  public FetchTask(
      @ThriftField(1) String address,
      @ThriftField(2) List<LogFileInfo> infos) {
    addr = address;
    fileInfos = infos;
  }

  @ThriftField(1)
  public String getAddr() {
    return addr;
  }

  @ThriftField(2)
  public List<LogFileInfo> getFileInfos() {
    return fileInfos;
  }

  public void addTask(LogFileInfo info) {
    fileInfos.add(info);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Requests for peer ").append(addr).append(" :");
    for (LogFileInfo info : fileInfos) {
      sb.append(" {").append(info.getFilename()).append(",[")
          .append(info.getInitialIndex()).append(",")
          .append(info.getLastIndex()).append("]}");
    }

    return sb.toString();
  }

  @Override
  public void run() {
    // TODO in part 2: fetch log files from the peer!
  }
}
