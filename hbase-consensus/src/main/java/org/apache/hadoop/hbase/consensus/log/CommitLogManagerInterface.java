package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface CommitLogManagerInterface {

  public final static EditId UNDEFINED_EDIT_ID = new EditId(HConstants.UNDEFINED_TERM_INDEX,
    HConstants.UNDEFINED_TERM_INDEX);

  /** Append the txns for this editId; Return false if the append fails */
  public boolean append(EditId editId, long commitIndex, final ByteBuffer data);

  /** Return whether this editId exists in the transaction log */
  public boolean isExist(EditId editId);

  /** Truncate the log up to this editId. Return false if the editId does not
   * exist. */
  public boolean truncate(EditId editId) throws IOException;

  /** Return the previous editId before the editId; Return null if this editId
   * does not exist */
  public EditId getPreviousEditID(EditId editId);

  /** Return the last editId in the log */
  public EditId getLastEditID();

  /** Return the first editId in the log */
  public long getFirstIndex();

  public void initialize(final ImmutableRaftContext context);

  public boolean isAccessible();

  public Pair<EditId, MemoryBuffer> getNextEditIdTransaction(
      final String sessionKey,
      final long currentIndex,
      final Arena arena) throws IOException;

  public EditId getLastValidTransactionId();

  public LogState getLogState();

  public String dumpLogs(int n);

  public String getPath();

  public EditId getEditId(final long index);

  public Pair<EditId, EditId> greedyIncorporateCandidateLogs(
      String    sessionKey,
      long      lastLogIndex
  );

  void fillLogGap(long seedIndex) throws IOException;

  public List<LogFileInfo> getCommittedLogStatus(long minIndex);

  public void stop();
}
