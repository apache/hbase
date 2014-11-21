package org.apache.hadoop.hbase.consensus.server.peer.states;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.FSMLargeOpsExecutorService;
import org.apache.hadoop.hbase.consensus.log.TransactionLogManager;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendRequestEvent;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendResponseEvent;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

public class PeerHandleAppendResponse extends PeerServerAsyncState {
  private static Logger LOG = LoggerFactory.getLogger(PeerHandleAppendResponse.class);
  private static int globalMaxBatchLogs = HConstants.RAFT_BATCH_APPEND_MAX_EDITS_DEFAULT;
  private static long globalMaxBatchBytes = HConstants.RAFT_BATCH_APPEND_MAX_BYTES_DEFAULT;
  private static boolean batchSameTerm = false;
  private final List<MemoryBuffer> buffers = new ArrayList<>();
  private final Arena arena;
  private ListenableFuture<?> createAppendRequestFromLogFuture = null;
  private static long maxwaitToReadFromDiskNs;

  public PeerHandleAppendResponse(PeerServerMutableContext context) {
    super(PeerServerStateType.HANDLE_APPEND_RESPONSE, context);
    arena = context.getQuorumContext().getArena();
    globalMaxBatchLogs = getConf().getInt(
      HConstants.CONSENSUS_PUSH_APPEND_MAX_BATCH_LOGS_KEY,
      globalMaxBatchLogs);
    globalMaxBatchBytes = getConf().getLong(
      HConstants.CONSENSUS_PUSH_APPEND_MAX_BATCH_BYTES_KEY,
      globalMaxBatchBytes);
    batchSameTerm = getConf().getBoolean(
      HConstants.CONSENSUS_PUSH_APPEND_BATCH_SAME_TERM_KEY,
      false);
    maxwaitToReadFromDiskNs =
        getConf().getLong(HConstants.MAXWAIT_TO_READ_FROM_DISK_NS,
          HConstants.MAXWAIT_TO_READ_FROM_DISK_NS_DEFAULT);
  }

  public static void setGlobalMaxBatchLogs(int n) {
    globalMaxBatchLogs = Math.max(1, n);
  }

  private Configuration getConf() {
    return c.getQuorumContext().getConf();
  }

  /**
   *  batchSameTerm
   *
   *  Whether to batch edits that have the same term.
   */
  public boolean batchSameTerm() {
    return batchSameTerm;
  }

  public int getMaxBatchLogs() {
    return globalMaxBatchLogs;
  }

  public long getMaxBatchBytes() {
    return globalMaxBatchBytes;
  }

  @Override
  public boolean isComplete() {
    return (createAppendRequestFromLogFuture == null ||
      createAppendRequestFromLogFuture.isDone());
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    return createAppendRequestFromLogFuture;
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    createAppendRequestFromLogFuture = null;

    // Nothing to do if you are not the leader
    if (!c.getQuorumContext().isLeader()) {
      return;
    }

    // Add the event to the raft state machine
    // update the last successfully committed edit by the peer
    if (e instanceof PeerAppendResponseEvent) {
      final AppendResponse response = ((PeerAppendResponseEvent)e).getAppendResponse();
      if (response != null) {
        c.setLastEditID(response.getPrevEditID());
        // Update the peer's availability
        c.updatePeerAvailabilityStatus(true);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(c + " got null as AppendResponse");
        }
      }
    }

    final EditId startEditId = c.getLastEditID();
    final EditId logManagerLastEditID =
      c.getQuorumContext().getLogManager().getLastEditID();

    // If the log manager has new entries
    if (logManagerLastEditID.compareTo(startEditId) > 0) {
      Runnable createAppendRequestRunnable = new Runnable() {
        @Override
        public void run() {
          AppendRequest nextRequest = createAppendRequestFromLog(startEditId,
            logManagerLastEditID);

          if (nextRequest != null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(c + " is sending " + nextRequest);
            }
            c.enqueueEvent(new PeerAppendRequestEvent(nextRequest));
          }
        }
      };

      createAppendRequestFromLogFuture =
        FSMLargeOpsExecutorService.submitToReadOpsThreadPool(
          createAppendRequestRunnable);
    } else if (c.getLatestRequest() != null &&
      c.getLatestRequest().getLogId(0).compareTo(startEditId) > 0) {
      // We have seen new entries which might have not been made in the log manager.
      // Lets send it.
      AppendRequest nextRequest = c.getLatestRequest();
      if (nextRequest != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(c + " is sending " + nextRequest);
        }
        c.enqueueEvent(new PeerAppendRequestEvent(nextRequest));
      }
    }
  }

  /**
   * Depending upon the batching parameters, it fetches the next set of transactions
   * for the log manager.
   *
   * @param startEditId EditId after which, transactions will be fetched
   * @param logManagerLastEditID latest EditId reported by the log manager
   * @return AppendRequest, or null in case of error
   */
  private AppendRequest createAppendRequestFromLog(final EditId startEditId,
                                                   final EditId logManagerLastEditID) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(c + " adding lagging entries starting from " + startEditId +
        "; log manager has " + logManagerLastEditID);
    }

    if (arena != null) {
      // Reset the buffers as we are going to read new transactions
      for (MemoryBuffer buffer : buffers) {
        arena.freeByteBuffer(buffer);
      }
    }

    buffers.clear();

    long start = System.nanoTime();

    // If the peer is at a lesser or equal term as yours and if there are more
    // entries present in the log, then send the missing transactions.
    List<ByteBuffer> edits = new ArrayList<>();
    List<EditId> logIds = new ArrayList<EditId>();
    long nbytes = 0;

    EditId lastEditID = startEditId;
    int maxEdits = getMaxBatchLogs();
    long maxBytes = getMaxBatchBytes();

    ConsensusHost leader = c.getQuorumContext().getLeader();
    // we could check if leader corresponds to c.getQuorumContext().
    // But it's unnecessary.
    collect_edits_loop:
    while (c.getQuorumContext().isLeader() &&
      logManagerLastEditID.compareTo(lastEditID) > 0 &&
      nbytes < maxBytes && edits.size() < maxEdits &&
      // time taken so far is smaller than defined threshold
        (System.nanoTime() - start) < maxwaitToReadFromDiskNs) {
      try {
        Pair<EditId, MemoryBuffer> idTxn =
          c.getQuorumContext().getLogManager().getNextEditIdTransaction(
          c.toString(), lastEditID.getIndex(), arena);

        if (idTxn == null ||
          idTxn.getFirst() == null ||
          idTxn.getFirst().equals(TransactionLogManager.UNDEFINED_EDIT_ID)) {
          LOG.error(c.getQuorumContext() + " could not locate next id after "
            + lastEditID + ". Returning.." + idTxn);
          break collect_edits_loop;
        }

        buffers.add(idTxn.getSecond());
        // We successfully found the entry, lets update the sharedPoolBuffer
        EditId nextEditId = idTxn.getFirst();

        // we currently cannot handle logs with differing terms on the receiver side
        if (batchSameTerm() && logIds.size() > 0 &&
          nextEditId.getTerm() != logIds.get(0).getTerm()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("---- " + c + " stop at lagging entry " + nextEditId +
              "[prev=" + lastEditID + "] due to changing terms from "
              + logIds.get(0));
          }
          break collect_edits_loop;
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("---- " + c + " trying to add lagging entry: " +
              nextEditId + "[prev=" + lastEditID + "]");
          }
        }

        ByteBuffer edit = idTxn.getSecond().getBuffer();
        edits.add(edit);
        logIds.add(nextEditId);
        nbytes += edit.remaining();
        lastEditID = nextEditId;
      } catch (IOException ex) {
        LOG.error(
          c.getQuorumContext() + " is unable to read next transaction " +
            "for previous id " + lastEditID + " for " + c, ex);
        break collect_edits_loop;
      }
    }

    if (edits.size() == 0) {
      LOG.warn("NOTHING to APPEND!");
      return null;
    }

    if (!leader.equals(c.getQuorumContext().getLeader())) {
      // Note that this covers the following cases
      // 1. I'm a leader both before and after the loop; but there was at least
      // one re-election in between.
      // 2. I was a leader before the loop and no longer the leader afterwards.
      LOG.warn(c.getQuorumContext() + " aborts sending AppendRequest because " +
        "the leader has changed from " + leader + " to " +
        c.getQuorumContext().getLeader());
      return null;
    }

    long batchCommitIndex = Math.max(logIds.get(logIds.size() - 1).getIndex(),
      c.getQuorumContext().getCommittedEdit().getIndex());

    // Update the metrics
    long elapsed = System.nanoTime() - start;
    c.getMetrics().getBatchRecoverySize().add(logIds.size());
    c.getMetrics().getBatchRecoveryLatency().add(elapsed, TimeUnit.NANOSECONDS);

    // Create request from nextEditId;
    return (new AppendRequest(
      c.getQuorumContext().getQuorumName(),
      leader,
      false,
      batchCommitIndex,
      c.getQuorumContext().getMinUnPersistedIndexAcrossQuorum(),
      startEditId,
      logIds,
      edits
    ));
  }
}
