package org.apache.hadoop.hbase.consensus.raft.states;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.FSMLargeOpsExecutorService;
import org.apache.hadoop.hbase.consensus.log.CommitLogManagerInterface;
import org.apache.hadoop.hbase.consensus.log.LogFileInterface;
import org.apache.hadoop.hbase.consensus.log.TransactionLogManager;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.raft.events.AppendRequestEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class HandleAppendRequest extends RaftAsyncState {
  private static Logger LOG = LoggerFactory.getLogger(HandleAppendRequest.class);
  private final boolean candidateLogPromotionEnabled;
  private final long raftBatchAppendTryCandidateLogsPromotionThresholdDefault;
  private volatile SettableFuture<Void> handleAppendReqFuture;

  public HandleAppendRequest(MutableRaftContext context) {
    super(RaftStateType.HANDLE_APPEND_REQUEST, context);
    candidateLogPromotionEnabled = c.getConf().getBoolean(
        HConstants.RAFT_CANDIDATE_LOG_PROMOTION_ENABLED_KEY,
        HConstants.RAFT_CANDIDATE_LOG_PROMOTION_ENABLED_DEFAULT);
    raftBatchAppendTryCandidateLogsPromotionThresholdDefault =
        c.getConf().getLong(
          HConstants.RAFT_BATCH_APPEND_TRY_CANDIDATE_LOGS_PROMOTION_THRESHOLD_KEY,
          HConstants.RAFT_BATCH_APPEND_TRY_CANDIDATE_LOGS_PROMOTION_THRESHOLD_DEFAULT);
  }

  private boolean candidateLogPromotionEnabled() {
    return candidateLogPromotionEnabled;
  }

  private static class SingleAppendResult {
    private int which;
    private AppendResponse.Result result;
    private EditId  validId;

    public SingleAppendResult(int which, AppendResponse.Result result, EditId validId) {
      update(which, result, validId);
    }
    public void update(int which, AppendResponse.Result result, EditId validId) {
      this.which = which;
      this.result = result;
      this.validId = validId;
    }

    public AppendResponse.Result getResult() {
      return result;
    }

    public EditId validId() {
      return validId;
    }

    public int getWhich() {
      return which;
    }
  }

  @Override
  public boolean isComplete() {
    return handleAppendReqFuture == null || handleAppendReqFuture.isDone();
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    return handleAppendReqFuture;
  }

  @Override
  public void onEntry(final Event event) {
    super.onEntry(event);
    handleAppendReqFuture = null;
    final long start = System.nanoTime();
    AppendRequestEvent e = (AppendRequestEvent) event;
    final AppendRequest request = e.getRequest();
    if (LOG.isTraceEnabled()) {
      LOG.trace(c + " processing AppendRequest " + request);
    }

    assert request != null;
    assert request.validateFields();

    EditId prevLogId = request.getPrevLogId();

    boolean promotedSomeCandidateLogs = false;
    // We poke the Candidate Logs Manager first.
    // Note we use the global commit index here. Current LogId could be very, very far
    // behind the global commit index.
    if (candidateLogPromotionEnabled() &&
      prevLogId.getIndex() + getTryPromotingCandidateLogsThreshold() < request.getCommitIndex()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(c + " . try promoting candidate logs before appends");
      }
      promotedSomeCandidateLogs = tryToIncorporateCandidateLogs(request.getCommitIndex());
    } else if (LOG.isTraceEnabled()) {
      LOG.trace(c + " . not promoting candidate logs.");
    }

    final SingleAppendResult lastResult = new SingleAppendResult(0, AppendResponse.Result.LAGGING, null);

    // Check if we can apply the edits.
    if (canApplyEdit(request, 0, prevLogId, lastResult)) {
      handleAppendReqFuture = SettableFuture.create();
      // Stop the timer, because we are actually making some progress.
      c.getProgressTimer().stop();
      final boolean promotedSomeCandidatesLogsFinal = false;
      ListenableFuture<EditId> applyAllEditsFuture = applyAllEdits(request, prevLogId, lastResult);
      Futures.addCallback(applyAllEditsFuture, new FutureCallback<EditId>() {

        @Override
        public void onSuccess(EditId result) {
          c.getProgressTimer().start();
          finish(start, request, result, promotedSomeCandidatesLogsFinal, lastResult);
          handleAppendReqFuture.set(null);
        }

        @Override
        public void onFailure(Throwable t) {
          c.getProgressTimer().start();
          request.setError(t);
          LOG.error("applyAllEdits failed for quorum: " + c.getQuorumInfo().getQuorumName(), t);
          handleAppendReqFuture.set(null);
        }
      });
    } else {
      finish(start, request, prevLogId, promotedSomeCandidateLogs, lastResult);
    }
  }

  /**
   * @param start
   * @param request
   * @param prevLogId
   * @param promotedSomeCandidateLogs
   * @param lastResult
   */
  private void finish(long start, final AppendRequest request, EditId prevLogId,
      boolean promotedSomeCandidateLogs, SingleAppendResult lastResult) {
    request.setResponse(
      new AppendResponse(
        c.getMyAddress(),
        request.getLogId(0), // 1st EditID in the request is the identifier for the AppendSession
        lastResult.validId(),
        lastResult.getResult(),
        c.getRanking(),
        c.canTakeOver() // can takeover
      ));

    // Add request latency
    c.getConsensusMetrics().getAppendEntriesLatency().add(
      System.nanoTime() - start, TimeUnit.NANOSECONDS);

    // We may have skipped log promotion because of some gaps. If that's the case, after we append
    // a few edits, we may be able to promote some candidate logs. Do this now and we will save
    // another round trip from the leader.
    if (lastResult.getResult() == AppendResponse.Result.SUCCESS &&
      candidateLogPromotionEnabled() &&
      !promotedSomeCandidateLogs &&
      prevLogId.getIndex() + getTryPromotingCandidateLogsThreshold() < request.getCommitIndex()
      ) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(c + " . try promoting candidate logs after successful appends");
      }
      promotedSomeCandidateLogs = tryToIncorporateCandidateLogs(request.getCommitIndex());
    }
  }

  /**
   * @param request
   * @param prevLogId
   * @param lastResult
   * @return
   * @throws Throwable
   */
  private ListenableFuture<EditId> applyAllEdits(final AppendRequest request, final EditId prevLogId,
      final SingleAppendResult lastResult) {
    return FSMLargeOpsExecutorService.fsmWriteOpsExecutorService.submit(new Callable<EditId>() {
      @Override
      public EditId call() throws Exception {
        EditId prevLogIdNonFinal = prevLogId;
        try {
          // we take each log in the list and try to apply it locally.
          for (int i = 0; i < request.logCount(); i++) {
            EditId logId = request.getLogId(i);
            if (!applySingleEdit(request, i, prevLogIdNonFinal, lastResult)) {
              break;
            }
            prevLogIdNonFinal = logId;
          }
        } catch (Throwable t) {
          throw t;
        }
        return prevLogIdNonFinal;
      }
    });

  }

  private boolean canApplyEdit(
    final AppendRequest request,
    final int which,
    final EditId prevLogId,
    SingleAppendResult result) {
    boolean appendResult = true;
    if (request.getLeaderId().getTerm() < c.getCurrentEdit().getTerm() ||
      !c.validateLogEntry(prevLogId)) {
      // When the current replica is lagging or higher term
      EditId lastLoggedEditID;
      // Reset the timer only if you are lagging entries. In case the leader's
      // term is less than yours, than don't count this request towards progress.
      if (request.getLeaderId().getTerm() >= c.getCurrentEdit().getTerm()) {
        c.getProgressTimer().reset();
      }

      // If old term, or previous edit does not match
      appendResult = false;
      lastLoggedEditID = c.getLogManager().getLastValidTransactionId();

      if (request.getLeaderId().getTerm() < c.getCurrentEdit().getTerm()) {
        // when the current peer has a higher term
        result.update(which, AppendResponse.Result.HIGHER_TERM, lastLoggedEditID);
      } else {
        // when the current peer is lagging
        result.update(which, AppendResponse.Result.LAGGING, lastLoggedEditID);
      }
    }

    EditId prevEdit = request.getLogId(0);
    for (int i = 1; i < request.logCount(); i++) {
      EditId curEdit = request.getLogId(i);
      if (curEdit.getIndex() != prevEdit.getIndex() + 1) {
        result.update(which, AppendResponse.Result.MISSING_EDITS,
          c.getLogManager().getLastValidTransactionId());
        LOG.error("Missing edits in request " + request +
          ". The next edit after " + prevEdit + " was " + curEdit);
        appendResult = false;
        break;
      }
      prevEdit = curEdit;
    }

    return appendResult;
  }

  /*
   *  applySingleEdit
   *
   *  Apply only one log from AppendRequest.
   */
  private boolean applySingleEdit(
      final AppendRequest       request,
      final int                 which,
      final EditId              prevLogId,
            SingleAppendResult  result   // simulate C-style passing by reference
  ) {
    boolean appendResult = true;
    EditId currEditID = request.getLogId(which);

    // request.getLeaderId().getTerm() >= c.getCurrentEdit().getTerm() &&
    // c.validateLogEntry(prevLogId)

    TransactionLogManager logManager = (TransactionLogManager)c.getLogManager();
    boolean needCheckInvariants = false;
    // If the leader receives a valid append request with higher term, print debug message and
    // check integrity of its hlogs.
    if (c.isLeader() && !c.getLeader().equals(request.getLeaderId())) {
      Map.Entry<Long, LogFileInterface> currentLogEntry = logManager.getUncommittedLogs().lastEntry();
      LogFileInterface currentLog = null;
      if (currentLogEntry != null) {
        currentLog = currentLogEntry.getValue();
      }
      LOG.warn(
          String.format("Received a higher term append request from %s when I'm the leader. "
                      + "Current edit: %s. Request: %s. Current log info: %s",
              request.getLeaderId(), c.getCurrentEdit(), request,
              currentLog == null ? "null" : currentLog.toString()));
      try {
        logManager.checkInvariants();
      } catch (IOException e) {
        LOG.error("Logs are messed up!", e);
        return false;
      }
      needCheckInvariants = true;
    }
    c.setLeader(request.getLeaderId());
    c.clearVotedFor();

    c.setLastAppendRequestReceivedTime(System.currentTimeMillis());
    c.setMinAckedIndexAcrossAllPeers(request.getPersistedIndex());

    // The commit index in the request is the highest in the entire batch. Hence
    // for this request, we should take the min of current logId and commit index.
    long commitIndex = Math.min(currEditID.getIndex(), request.getCommitIndex());

    // isExist() takes the read flow which grabs a lock while doing a lookup. One
    // small optimization is to check if the current index is less than the
    // input index. We are safe to do the check here as we have already verified
    // that the previous edit id is present in the log
    if (c.getCurrentEdit().getIndex() < currEditID.getIndex() ||
      !c.getLogManager().isExist(currEditID)) {
      // Reset the leader which will ask the current leader/candidate to stepdown in the
      // BecomeFollower state
      if (!request.isHeartBeat()) {
        try {
          c.truncateLogEntries(prevLogId);
          ByteBuffer edit = request.getEdit(which);
          c.appendToLog(currEditID, commitIndex, edit);
          c.setPreviousEditId(currEditID);
          c.setCurrentEditId(currEditID);
          if (QuorumInfo.isQuorumChangeRequest(edit)) {
            LOG.debug("Received a quorum change request. " +
              "Caching it for now, will handle later. EditId: " + currEditID);
          }
        } catch (IOException ex) {
          LOG.error("Cannot append edit " + request.getLogId(which), ex);
          appendResult = false;
        }
        if (appendResult == false) {
          // return the last edit present in the log manager
          currEditID = c.getLogManager().getLastEditID();
        }
      }
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace(c + " . Transaction already exists: " + currEditID);
      }
      if (!request.isHeartBeat()) {
        c.getConsensusMetrics().incAppendEntriesDuplicates();
      }
    }

    if (needCheckInvariants) {
      // Check invariants again after the append is done
      try {
        logManager.checkInvariants();
      } catch (IOException e) {
        LOG.error("Logs are messed up!", e);
        return false;
      }
    }

    // Move forward the commit index. This should be done regardless of
    // whether the current message is a heartbeat or not. The information
    // about the commitIndex is piggy backed on the current message. But,
    // does not depend on the current message in any way.
    c.advanceCommittedIndex(new EditId(request.getLogId(which).getTerm(),
      commitIndex));

    if (appendResult) {
      result.update(which, AppendResponse.Result.SUCCESS, currEditID);
    } else {
      result.update(which, AppendResponse.Result.LAGGING, currEditID);
    }
    return appendResult;
  }


  /**
   *  tryToIncorporateCandidateLogs
   *
   *  Trying to incorporate some candidate log files given current append request.
   */
  public boolean tryToIncorporateCandidateLogs(long lastCommittedIndex) {
    CommitLogManagerInterface logman = c.getLogManager();
    long t0 = System.currentTimeMillis();
    Pair<EditId, EditId> result = logman.greedyIncorporateCandidateLogs(c.toString(), lastCommittedIndex);
    if (result != null && result.getSecond() != null) {
      EditId prevEditId = result.getFirst();
      EditId lastEditId = result.getSecond();
      long prevIndex = prevEditId != null ?
        (prevEditId.getIndex() != HConstants.UNDEFINED_TERM_INDEX ? prevEditId.getIndex() : 0) : 0;
      LOG.info("Incorporated " + (lastEditId.getIndex() - prevIndex) + " edits in "
          + (System.currentTimeMillis() - t0) + " milliseconds.");
      c.setPreviousEditId(lastEditId);
      c.setCurrentEditId(lastEditId);
      return true;
    }
    return false;
  }

  public long getTryPromotingCandidateLogsThreshold() {
    return raftBatchAppendTryCandidateLogsPromotionThresholdDefault;
  }
}
