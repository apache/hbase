package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsReplicationGlobalSourceSource extends MetricsReplicationSourceSource {

  public static final String SOURCE_WAL_READER_EDITS_BUFFER = "source.walReaderEditsBufferUsage";

  /**
   * Sets the total usage of memory used by edits in memory read from WALs.
   * @param usage The memory used by edits in bytes
   */
  void setWALReaderEditsBufferBytes(long usage);
  /**
   * Returns the size, in bytes, of edits held in memory to be replicated.
   */
  long getWALReaderEditsBufferBytes();
}
