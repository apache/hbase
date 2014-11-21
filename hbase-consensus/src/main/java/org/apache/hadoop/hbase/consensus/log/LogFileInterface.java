package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

public interface LogFileInterface {
  public long getInitialIndex();
  public long getLastIndex();
  public long getCurrentTerm();
  public MemoryBuffer getTransaction(
    long term, long index, String sessionKey, final Arena arena)
    throws IOException, NoSuchElementException;
  public long getLastModificationTime();
  public void closeAndDelete() throws IOException;
  public long getTxnCount();
  public String getFileName();
  public File getFile();
  public long getFileSize();
  public String getFileAbsolutePath();
  public long getCreationTime();
}
