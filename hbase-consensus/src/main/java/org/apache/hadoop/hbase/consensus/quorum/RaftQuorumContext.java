package org.apache.hadoop.hbase.consensus.quorum;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.exceptions.NewLeaderException;
import org.apache.hadoop.hbase.consensus.fsm.ConstitutentFSMService;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.FSMLargeOpsExecutorService;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachine;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachineService;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachineServiceImpl;
import org.apache.hadoop.hbase.consensus.fsm.State;
import org.apache.hadoop.hbase.consensus.fsm.Util;
import org.apache.hadoop.hbase.consensus.log.CommitLogManagerInterface;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.log.TransactionLogManager;
import org.apache.hadoop.hbase.consensus.metrics.ConsensusMetrics;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.protocol.Payload;
import org.apache.hadoop.hbase.consensus.raft.RaftStateMachine;
import org.apache.hadoop.hbase.consensus.raft.events.ProgressTimeoutEvent;
import org.apache.hadoop.hbase.consensus.raft.events.RaftEventType;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.raft.events.ReseedRequestEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.metrics.TimeStat;
import org.apache.hadoop.hbase.regionserver.DataStoreState;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.BucketAllocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InHeapArena;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold RAFT Consensus related data for each replica/peer.
 */
public class RaftQuorumContext implements ImmutableRaftContext,
  MutableRaftContext {
  private static final Logger LOG = LoggerFactory.getLogger(
    RaftQuorumContext.class);

  /** Single global timer to schedule events */
  private final AggregateTimer aggregateTimer;

  /** Event execution stream engine */
  private final SerialExecutorService serialExecutor;

  /** Lowest progress timeout value in the millis */
  volatile long progressTimeoutForLowestRankMillis;

  /** The follower/candidate progress timer */
  private Timer progressTimer;

  /** Total time in ms to retry the append entry */
  private int appendEntriesTimeoutInMilliseconds;

  /** The transaction logManager */
  private CommitLogManagerInterface logManager;

  /** Current leader */
  private ConsensusHost leader;

  /** Last host we voted for */
  private ConsensusHost lastVotedFor;

  /** Consensus Server address */
  private final HServerAddress localConsensusServerAddress;

  /** HRegionServer address */
  protected final HServerAddress regionServerAddress;

  /** Peer Address */
  private final String myAddress;

  /** Current Edit */
  private EditId currentEdit;

  /** Latest committed Edit */
  private EditId committedEdit;

  /** Previous Edit */
  private EditId previousLogEdit;

  /** Time we last received an append request */
  private long lastAppendRequestTimeMillis = -1;

  /** Session object for the current outstanding Append Session */
  private AppendConsensusSessionInterface outstandingAppendSession;

  /** Session object for the current outstanding Election Session */
  private VoteConsensusSessionInterface outstandingElectionSession;

  /** Map of the last set of AppendEntries applied to log but not committed */
  private HashMap<Long, Payload> uncommittedEntries;

  /** Heartbeat Timer */
  private Timer heartbeatTimer;

  /** The majority cnt in the quorum including the current server */
  private int majorityCnt;

  /** Manages the set of peers and their associated states */
  protected PeerManagerInterface peerManager;

  /** After successfully moving into the new quorum, close agents to
   * old peers because we no longer need them */
  JointConsensusPeerManager oldManager;

  /** Information about the HBase Region and
   * peers in quorum.
   */
  protected QuorumInfo quorumInfo;

  /** Current update Membership Change Request */
  private QuorumMembershipChangeRequest updateMembershipRequest;

  /**
   * If there is an impending quorum change, and if we are no longer going to be
   * a part of the quorum eventually, note it down. Used when the quorum moves
   * to the new configuration, without letting us know (so that we can
   * shutdown).
   */
  private volatile boolean partOfNewQuorum = true;

  /** Data Store to committ entries to */
  protected RaftEventListener dataStoreEventListener;

  /** HRegion Server configuration object */
  private final Configuration conf;

  /** Agent others should use to send requests to this replica */
  private QuorumAgent agent;

  /** State Machine */
  private FiniteStateMachineService stateMachine;

  /** Memory Allocator. Disabled by default */
  private final Arena arena;

  /** The rank is occasionally read outside the FSM so it should be volatile */
  protected volatile int rank;

  /** Consensus Metrics Object */
  protected ConsensusMetrics metrics;

  private final Random random;

  /** Location of the file which store the lastVotedFor persistently on disk */
  private Path lastVotedForFilePath;

  /** Handle to lastVoteFor file */
  private RandomAccessFile lastVotedForFile;

  /** Allow balancing in case higher rank replica is available */
  private final boolean enableStepdownOnHigherRankConf;

  /** Whether the current replica is ready to take over the leadership or not */
  private volatile boolean canTakeOver = false;

  /** Amount of time to wait before we become ready to takeover */
  private long takeOverReadyTime = 0;

  private boolean useAggregateTimer;

  private long lastAppendRequestReceivedTime = 0;

  private ExecutorService execServiceForThriftClients;

  public RaftQuorumContext(final QuorumInfo info,
                           final Configuration config,
                           final HServerAddress consensusServerAddress,
                           final String metricsMBeanNamePrefix,
                           final AggregateTimer aggregateTimer,
                           final SerialExecutorService serialExecutor,
                           final ExecutorService execServiceForThriftClients) {

    quorumInfo = info;
    conf = config;
    this.metrics = new ConsensusMetrics(
            metricsMBeanNamePrefix + info.getQuorumName(),
            consensusServerAddress.getHostNameWithPort().replace(':', '.'));
    this.aggregateTimer = aggregateTimer;
    this.serialExecutor = serialExecutor;
    random = new Random(System.currentTimeMillis());
    localConsensusServerAddress = consensusServerAddress;
    this.updateMembershipRequest = null;
    regionServerAddress = RaftUtil.getHRegionServerAddress(
      new HServerAddress(localConsensusServerAddress));
    myAddress = localConsensusServerAddress.getHostAddressWithPort();

    majorityCnt = info.getPeersWithRank().size() / 2 + 1 ;
    initializeRank();

    if (conf.getBoolean(HConstants.USE_ARENA_KEY, HConstants.USE_ARENA_DEFAULT)) {
      int capacity = config.getInt(HConstants.ARENA_CAPACITY_KEY,
        HConstants.ARENA_CAPACITY_DEFAULT);
      arena = new InHeapArena(BucketAllocator.DEFAULT_BUCKETS, capacity);
    } else {
      arena = null;
    }

    FSMLargeOpsExecutorService.initialize(config);

    this.enableStepdownOnHigherRankConf =
        conf.getBoolean(HConstants.ENABLE_STEPDOWN_ON_HIGHER_RANK_CAUGHT_UP, true);

    useAggregateTimer = conf.getBoolean(
      HConstants.QUORUM_USE_AGGREGATE_TIMER_KEY,
      HConstants.QUORUM_USE_AGGREGATE_TIMER_DEFAULT);
    this.execServiceForThriftClients = execServiceForThriftClients;
    LOG.debug("RaftQuorumContext for quorum " + getQuorumName() +
      " initialized with rank: " + getRanking());
    this.uncommittedEntries = new HashMap<>();
  }

  public void startStateMachines() {
    initializeMetaData();
    initializeAgent();
    initializePeers();
    initializeTimers();
    initializeStateMachine();
  }

  @Override
  public CommitLogManagerInterface getLogManager() {
    return logManager;
  }

  public void setStateMachineService(
          final FiniteStateMachineService stateMachineService) {
    stateMachine = stateMachineService;
  }

  public State getCurrentRaftState() {
    return stateMachine.getCurrentState();
  }

  /**
   * Tells whether the current peer is ready to takeover or not.
   * @return boolean ready to takeover or not
   */
  @Override
  public boolean canTakeOver() {
    if (!canTakeOver) {
      if (System.currentTimeMillis() > takeOverReadyTime) {
        canTakeOver = true;
        // now that we are ready for take over let us make sure to set our
        // timeout correctly.
        if (this.progressTimer != null) {
          this.progressTimer.setDelay(this.getProgressTimeoutForMeMillis(),
            TimeUnit.MILLISECONDS);
          LOG.info(String.format("%s Region is now ready to become the ACTIVE." +
              " Setting timeout to %d.", quorumInfo.getQuorumName(),
            this.getProgressTimeoutForMeMillis()));
        }
        return true;
      }
    }
    return canTakeOver;
  }

  /**
   * Initializes the log manager and updates the current and previous Edit info
   */
  public synchronized void initializeLog() {
    if (logManager == null) {
      logManager = new TransactionLogManager(
        conf, quorumInfo.getQuorumName(), HConstants.UNDEFINED_TERM_INDEX);
    }

    logManager.initialize(this);
    refreshLogState();
  }

  /**
   * Tells the replica to use the given index as the starting index in the log.
   *
   * @param index bootstrap index
   * @throws IOException
   */
  @Override
  public void reseedStartIndex(long index) throws IOException {
    logManager.fillLogGap(index);
    refreshLogState();
  }

  /**
   * Creates and initializes the peer state machines
   */
  protected void initializePeers() {
    if (peerManager == null) {
      peerManager = new SingleConsensusPeerManager(this);
    }
    peerManager.initializePeers();
  }

  /**
   * Initialize the QuorumAgent responsible for communicating with the clients
   */
  public void initializeAgent() {
    if (agent == null) {
      agent = new QuorumAgent(this);
    }
  }

  public void setLogManager(final CommitLogManagerInterface manager) {
    logManager = manager;
  }

  public void registerRaftEventListener(RaftEventListener listener) {
    this.dataStoreEventListener = listener;
  }

  @Override
  public void resetPeers() {
    peerManager.resetPeers();
  }

  @Override
  public void setPeerReachable(String address) {
    peerManager.setPeerReachable(address);
  }

  @Override
  public HServerAddress getServerAddress() {
    return regionServerAddress;
  }

  /**
   * Upgrade to Joint Quorum Membership from single Quorum membership.
   *
   * @param newConfig the new config
   * @throws IOException
   */
  @Override
  public void updateToJointQuorumMembership(final QuorumInfo newConfig)
    throws IOException {
    if (peerManager.isInJointQuorumMode()) {
      throw new IOException(String.format("Region %s is already in joint " +
        "quorum mode.", getQuorumName()));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getQuorumName() + " Updating to joint quorum membership");
    }

    SingleConsensusPeerManager oldManager = (SingleConsensusPeerManager)peerManager;
    JointConsensusPeerManager peerManager =
      new JointConsensusPeerManager(this, newConfig);
    peerManager.setOldPeerServers(oldManager.getPeerServers());
    peerManager.initializePeers();
    this.peerManager = peerManager;
    checkIfNotPartOfNewQuorum(newConfig);
  }

  /**
   * Move to the new quorum configuration from the current joint quorum
   * configuration.
   *
   * @param newConfig The new config to move to.
   * @throws IOException
   */
  @Override
  public void updateToNewQuorumMembership(final QuorumInfo newConfig)
    throws IOException {
    LOG.debug(getQuorumName() + " Updating to new quorum membership");
    if (!peerManager.isInJointQuorumMode()) {
      throw new IOException(String.format("Cannot upgrade %s to new quorum, " +
        "as it isn't in joint quorum mode.", getQuorumName()));
    }
    oldManager = (JointConsensusPeerManager)peerManager;

    // Verify that all the peers in the new config are in the new peer servers list
    for (HServerAddress peer : newConfig.getPeersWithRank().keySet()) {
      if (peer.getHostAddressWithPort().equals(
        this.regionServerAddress.getHostAddressWithPort())) {
        continue;
      }
      if (oldManager.getNewPeerServers().get(
        RaftUtil.getLocalConsensusAddress(peer).getHostAddressWithPort()) == null) {
        throw new IOException("Invalid list of new peers. Cannot update the" +
          " quorum to new config. Reason: Peer " + peer + " not present.");
      }
    }

    // Perform the swap
    setQuorumInfo(newConfig);
    SingleConsensusPeerManager newManager = new SingleConsensusPeerManager(this);

    newManager.setPeerServers(oldManager.getNewPeerServers());

    peerManager = newManager;
  }

  /**
   * This method checks if we are not a part of the new quorum. If we are not,
   * then it marks the 'partOfNewQuorum' boolean to false. This helps in
   * avoiding conditions where we miss any of the quorum membership change
   * messages.
   *
   * @param newQuorumConfig
   */
  public void checkIfNotPartOfNewQuorum(QuorumInfo newQuorumConfig) {
    if (!newQuorumConfig.getPeersWithRank().keySet().contains(getServerAddress())) {
      LOG.warn(
        String.format("%s on %s is not a member of the new quorum. " +
          "Will be closing in future. New Quorum Members: %s. " +
          "Setting partOfNewQuorum = false.",
          getQuorumName(), getMyAddress(),
          newQuorumConfig.getPeersWithRank().keySet())
      );
      partOfNewQuorum = false;
    }
  }

  @Override
  public void cleanUpJointStates() {
    // Stop the old peers
    oldManager.stopOldPeers();
    oldManager = null;
  }

  @Override
  public void handleQuorumChangeRequest(ByteBuffer buffer)
    throws IOException {
    List<QuorumInfo> configs = QuorumInfo.deserializeFromByteBuffer(buffer);
    if (configs == null) {
      return;
    }
    if (configs.size() == 1) {
      updateToNewQuorumMembership(configs.get(0));
    } else {
      updateToJointQuorumMembership(configs.get(1));
    }

    // Close the data store if this replica is not part of the replica
    // set anymore after this change request.
    if (getQuorumInfo().getPeersWithRank().get(getServerAddress()) == null) {
      LOG.info("Current host " + getServerAddress() + " is no longer a member"
        + " of the quorum " + getQuorumName() + ". Stopping the context!");
      stop(false);
      if (getDataStoreEventListener() != null) {
        getDataStoreEventListener().closeDataStore();
      } else {
        LOG.warn("Data store not closed as no event listener could be found");
      }
    }
  }

  protected void initializeTimers() {
    progressTimeoutForLowestRankMillis = conf.getInt(
            HConstants.PROGRESS_TIMEOUT_INTERVAL_KEY,
            HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS);

    appendEntriesTimeoutInMilliseconds = conf.getInt(
      HConstants.APPEND_ENTRIES_TIMEOUT_KEY,
      HConstants.DEFAULT_APPEND_ENTRIES_TIMEOUT_IN_MILLISECONDS);

    // Check if we can takeover from start itself.
    canTakeOver();

    if (heartbeatTimer == null) {
      setHeartbeatTimer(
        RaftUtil.createTimer(
          useAggregateTimer,
          "heartbeat",
          getHeartbeatTimeoutInMilliseconds(),
          TimeUnit.MILLISECONDS,
          new HeartbeatTimeoutCallback(this),
          aggregateTimer
        )
      );
    }

    if (progressTimer == null) {
      setProgressTimer(
        RaftUtil.createTimer(
          useAggregateTimer,
          "progress",
          getProgressTimeoutForMeMillis(),
          TimeUnit.MILLISECONDS,
          new ProgressTimeoutCallback(this),
          aggregateTimer
        )
      );
    }
  }

  /**
   * Initialize the consensus with the quorum configuration, last term,
   * last index, last committed index, vote_for and so on.
   *
   * Initialize the livenessTimer thread.
   *
   * Make a transition to Follower state.
   */
  public void initializeAll(long seedIndex) {
    initializeLog();
    startStateMachines();
  }

  public QuorumAgent getQuorumAgentInstance() {
    return agent;
  }

  @Override
  public Arena getArena() {
    return arena;
  }

  @Override
  public boolean isLeader() {
    return (
      (leader != null) &&
      (lastVotedFor != null) &&
      myAddress.equals(leader.getHostId()) &&
      leader.equals(lastVotedFor) &&
      (currentEdit.getTerm() == leader.getTerm()) &&
      logManager.isAccessible()
    );
  }

  @Override
  public boolean isCandidate() {
    return (
      (lastVotedFor != null) &&
      myAddress.equals(lastVotedFor.getHostId()) &&
      (leader == null || !leader.getHostId().equals(lastVotedFor.getHostId())) &&
      (currentEdit.getTerm() == lastVotedFor.getTerm()) &&
      logManager.isAccessible()
    );
  }

  @Override
  public boolean isFollower() {
    return (!isLeader() && !isCandidate() && logManager.isAccessible());
 }

  @Override
  public String getMyAddress() {
    return this.myAddress;
  }

  @Override
  public EditId getCurrentEdit() {
    return currentEdit;
  }

  @Override
  public EditId getCommittedEdit() {
    return committedEdit;
  }

  @Override
  public EditId getPreviousEdit() {
    return previousLogEdit;
  }

  @Override
  public ConsensusHost getLeader() {
    return leader;
  }

  @Override
  public ConsensusHost getLastVotedFor() {
    return lastVotedFor;
  }

  @Override
  public ConsensusMetrics getConsensusMetrics() {
    return metrics;
  }

  @Override
  public Map<HServerAddress, Integer> getNewConfiguration() {
    if (updateMembershipRequest != null) {
      return updateMembershipRequest.getConfig().getPeersWithRank();
    }
    return null;
  }

  @Override
  public AppendConsensusSessionInterface getAppendSession(final EditId id) {
    if (outstandingAppendSession != null &&
      outstandingAppendSession.getSessionId().equals(id)) {
      return outstandingAppendSession;
    }
    return null;
  }

  @Override
  public VoteConsensusSessionInterface getElectionSession(final EditId id) {
    if (outstandingElectionSession != null &&
      outstandingElectionSession.getSessionId().equals(id)) {
      return outstandingElectionSession;
    }
    return null;
  }

  @Override
  public AppendConsensusSessionInterface getOutstandingAppendSession() {
    return outstandingAppendSession;
  }

  @Override
  public VoteConsensusSessionInterface getOutstandingElectionSession() {
    return outstandingElectionSession;
  }

  @Override
  public boolean isLogAccessible() {
    return logManager.isAccessible();
  }

  @Override
  public void setCurrentEditId(final EditId edit) {
    currentEdit = edit;
  }

  /**
   * Moves the committed index to request index.
   *
   * It will mark all the index from the current committedIndex to the input index
   * as committed. It also forward the request to the data store (if present)
   *
   * @param edit
   */
  @Override
  public void advanceCommittedIndex(final EditId edit) {

    // if the commit index is not initialized or if the new commit index is
    // greater than the current one then commit all the uncommitted entries so
    // far we have.
    if (committedEdit == null ||
      (edit.getIndex() >= committedEdit.getIndex() &&
       edit.getIndex() != HConstants.UNDEFINED_TERM_INDEX)) {
      // apply commits
      long nextIndex = committedEdit.getIndex() + 1;

      Payload payload;
      while (nextIndex <= edit.getIndex()) {
        payload = uncommittedEntries.get(nextIndex);
        if (payload != null) {
          if (QuorumInfo.isQuorumChangeRequest(payload.getEntries())) {
            // TODO @gauravm:
            // Make the state machine go to HALT if there is an exception.
            // Need to figure out why exactly can't we remove the
            // updateToNewQuorumMembership() call from the
            // ChangeQuorumMembership state, and handle all changes here.
            try {
              handleQuorumChangeRequest(payload.getEntries());
            } catch (IOException ioe) {
              LOG.error(String.format(
                "Could not apply the config change for quorum %s, because of: ",
                getQuorumName()), ioe);
            }
          }

          if (dataStoreEventListener != null) {
            this.dataStoreEventListener.commit(nextIndex, payload);
          } else if (payload.getResult() != null) {
            payload.getResult().set(nextIndex);
          }
          uncommittedEntries.remove(nextIndex);
        }
        ++nextIndex;
      }
      committedEdit = edit;
    }
  }

  @Override
  public void setLeader(ConsensusHost host) {
    clearLeader();
    leader = host;
  }

  @Override
  public void setPreviousEditId(final EditId id) {
    previousLogEdit = id;
  }

  @Override
  public ListenableFuture<Void> setVotedFor(ConsensusHost votedFor) {
    lastVotedFor = votedFor;
    return (ListenableFuture<Void>) FSMLargeOpsExecutorService.submitToWriteOpsThreadPool(
      new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          persistLastVotedFor();
          return null;
        }
      });
  }

  @Override
  public void clearLeader() {
    leader = null;
  }

  @Override
  public void clearVotedFor() {
    lastVotedFor = null;
    persistLastVotedFor();
  }

  @Override
  public void setAppendSession(AppendConsensusSessionInterface session) {
    outstandingAppendSession = session;
  }

  @Override
  public void setElectionSession(VoteConsensusSessionInterface session) {
    outstandingElectionSession = session;
  }

  /**
   * Appends the data which the given edit id to the transaction log.
   *
   * @param currLogId Edit Id for the data
   * @param commitIndex current committed Index
   * @param data the payload
   */
  @Override
  public void appendToLog(final EditId currLogId, final long commitIndex, final ByteBuffer data) {
    long now = System.currentTimeMillis();
    int sizeToWrite = data.remaining();
    try (TimeStat.BlockTimer timer = metrics.getLogWriteLatency().time()) {
      logManager.append(currLogId, commitIndex, data);
      final Payload payload =
          getAppendSession(currLogId) == null ? new Payload(data, null) : getAppendSession(
            currLogId).getReplicateEntriesEvent().getPayload();
      uncommittedEntries.put(currLogId.getIndex(), payload);
    } catch (Exception e) {
      LOG.error(this + "Error while appending entries to logManager ", e);
      assert !logManager.isAccessible();
    } finally {
      long timeTakenToAppendToLog = System.currentTimeMillis() - now;
      if (timeTakenToAppendToLog > 1000) {
        metrics.getLogAppendGreaterThanSecond().add(timeTakenToAppendToLog);
        LOG.warn("Time taken to append to log: " + timeTakenToAppendToLog
            + " is > 1000ms, and we are writing: " + sizeToWrite + " quorum: "
            + quorumInfo.getQuorumName());
      }
    }
  }

  @Override
  public int getMajorityCnt() {
    return majorityCnt;
  }

  @Override
  public void sendVoteRequestToQuorum(VoteRequest request) {
    peerManager.sendVoteRequestToQuorum(request);
  }

  /**
   * Broadcasts the AppendRequest to the peers in the Quorum
   * @param request
   */
  public void sendAppendRequestToQuorum(AppendRequest request) {
    peerManager.sendAppendRequestToQuorum(request);
  }

  /**
   * Truncates all the entries the transaction log greater that the given
   * valid edit.
   *
   * @param lastValidEntryId last valid EditId
   * @throws IOException
   */
  @Override
  public void truncateLogEntries(final EditId lastValidEntryId) throws IOException {
    try (TimeStat.BlockTimer timer = metrics.getLogTruncateLatency().time()) {
      if (!logManager.truncate(lastValidEntryId)) {
        throw new IOException("Failed to truncate logs since " + lastValidEntryId + "; rank "
            + rank);
      }
      long currentIndex = currentEdit.getIndex();
      while (currentIndex > lastValidEntryId.getIndex()) {
        uncommittedEntries.remove(currentIndex--);
      }
    }
  }

  @Override
  public int getRanking() {
    return rank;
  }

  /**
   * Tells whether the log has the request edit or not
   *
   * @param id EditId to check
   * @return
   */
  @Override
  public boolean validateLogEntry(final EditId id) {
    // If we are told to verify the start of log, or if the id is the same
    // as the in-memory previous log edit. In that case, we don't have to touch
    // the log manager.
    if (id.equals(TransactionLogManager.UNDEFINED_EDIT_ID) ||
        id.equals(previousLogEdit)) {
      return true;
    }
    return logManager.isExist(id);
  }

  @Override
  public boolean offerEvent(final Event e) {
    return stateMachine.offer(e);
  }

  @Override
  public Timer getHeartbeatTimer() {
    return heartbeatTimer;
  }

  public void setHeartbeatTimer(final Timer heartbeatTimer) {
    this.heartbeatTimer = heartbeatTimer;
  }

  @Override
  public Timer getProgressTimer() {
    return progressTimer;
  }

  public void setProgressTimer(final Timer progressTimer) {
    this.progressTimer = progressTimer;
  }

  @Override
  public String getQuorumName(){
    return getRegionNameAsQuorumName();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public EditId getLastLogIndex() {
    return previousLogEdit;
  }

  /**
   * Triggers the shutdown of the peer
   *
   * @param wait
   */
  @Override
  public void stop(boolean wait) {
    // Send a HALT event, causing the FSM to abort future events.
    LOG.debug("Offering the HALT event.");
    stateMachine.offer(new Event(RaftEventType.HALT));
    stateMachine.shutdown();

    progressTimer.shutdown();
    heartbeatTimer.shutdown();
    peerManager.stop();
    logManager.stop();

    // With the RaftStateMachine in HALT state the queue should be drained and
    // shutdown should happen quickly.
    if (wait && !Util.awaitTermination(
        stateMachine, 3, 3, TimeUnit.SECONDS)) {
      LOG.error("State Machine Service " + stateMachine.getName() +
              " did not shutdown");
    }
  }

  @Override
  public String toString() {
    return "[QuorumContext: " + myAddress + ", quorum: " + getQuorumName() +
      ", rank: " + rank + "]";
  }

  private String getRegionNameAsQuorumName(){
    return quorumInfo.getQuorumName();
  }

  @Override
  public QuorumInfo getQuorumInfo() {
    return quorumInfo;
  }

  @Override
  public ListenableFuture<?> sendAppendRequest(ReplicateEntriesEvent event) {
    return
      createAndSendAppendRequest(event);
  }

  @Override
  public ListenableFuture<?> sendEmptyAppendRequest() {
    ReplicateEntriesEvent event = new ReplicateEntriesEvent(
      false, ByteBuffer.allocate(1));
    return createAndSendAppendRequest(event);
  }

  /**
   * Will stepdown from the being the leader.
   *
   * It will also abort any outstanding AppendConsensusSessions.
   *
   */
  @Override
  public void leaderStepDown() {
    AppendConsensusSessionInterface session = this.getOutstandingAppendSession();

    if (session != null) {
      String leaderId = "";
      if (getLeader() != null && !getLeader().getHostId().equals(this.getMyAddress())) {
        leaderId = getLeader().getHostId();
      } else {
        // If the leader voluntarily step down, then clear the leader variable here.
        clearLeader();
      }

      if (dataStoreEventListener != null) {
        dataStoreEventListener.becameNonLeader();
      }

      // Set the replication event failed
      ReplicateEntriesEvent event = session.getReplicateEntriesEvent();
      if (event != null && !event.isHeartBeat()) {
        event.setReplicationFailed(new NewLeaderException(leaderId));
        uncommittedEntries.clear();
      }

      // We lost/gave up leadership, lets backs off a little bit
      if (session.getResult().equals(SessionResult.STEP_DOWN)) {
       long timeOut = progressTimeoutForLowestRankMillis +
          Math.abs(random.nextLong()) % progressTimeoutForLowestRankMillis;

        LOG.warn(this + " has stepped down from leader state and its last request " +
          session.getAppendRequest() + " and back off the timeout for " + timeOut + " ms");

        this.progressTimer.backoff(timeOut, TimeUnit.MILLISECONDS);
      }

      // Reset the outstanding AppendConsensusSession
      this.setAppendSession(null);
    }

    // Should not honor quorum change request in follower mode
    if (updateMembershipRequest != null) {
      updateMembershipRequest.setResponse(false);
      setUpdateMembershipRequest(null);
    }

    getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.FOLLOWER);
  }

  /**
   * Steps down from Candidate Role.
   *
   * It will abort any outstanding VoteConsensusSession.
   *
   */
  @Override
  public void candidateStepDown() {

    final VoteConsensusSessionInterface session = this.getElectionSession(
      this.getCurrentEdit());

    /**
     * Handle the scenario where you were a candidate and you didn't get the
     * majority as your log is lagging as compared to majority, but no one else
     * has higher term so far; We need to step down and decrease the
     * term to the previous one, so that we can join the quorum.
     */
    if (session != null) {
      this.setCurrentEditId(logManager.getLastEditID());

      // We lost an election, let's back off a little bit
      long timeout = Math.abs(random.nextLong()) % progressTimeoutForLowestRankMillis;
      LOG.info(this + " has stepped down from candidate state and its last request " +
        session.getRequest() + " and back off the timeout for " + timeout + " ms");
      this.progressTimer.backoff(timeout, TimeUnit.MILLISECONDS);
    }

    /** Reset election session */
    this.setElectionSession(null);

    getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.FOLLOWER);
  }

  /**
   * Resends the pending AppendRequest to the peers.
   */
  @Override
  public void resendOutstandingAppendRequest() {
    // Reset the heart beat timer
    getHeartbeatTimer().reset();

    // Reset the session and re-send the AppendRequest one more time
    outstandingAppendSession.reset();

    // increment ack count for yourself
    outstandingAppendSession.incrementAck(currentEdit, myAddress, rank, false);
    sendAppendRequestToQuorum(outstandingAppendSession.getAppendRequest());
  }

  public PeerStatus.RAFT_STATE getPaxosState() {
    return getConsensusMetrics().getRaftState();
  }

  public DataStoreState getStoreState() {
    return dataStoreEventListener.getState();
  }

  public LogState getLogState() {
    LogState logState = this.logManager.getLogState();
    logState.setPeerState(peerManager.getState());

    return logState;
  }

  public List<LogFileInfo> getCommittedLogStatus(long minIndex) {
    return logManager.getCommittedLogStatus(minIndex);
  }

  public Map<String, PeerServer> getPeerServers() {
    return peerManager.getPeerServers();
  }

  @Override
  public QuorumMembershipChangeRequest getUpdateMembershipRequest() {
    return updateMembershipRequest;
  }

  /**
   * Get the max safest index before which all the logs can be purged.
   *
   * It is basically is the
   * min(minIndexPersistedAcrossAllPeers, prevFlushMaxSeqId)
   *
   * @return long max safest index
   */
  @Override public long getPurgeIndex() {
    long minUncommittedIndexInMemstores = Long.MAX_VALUE;
    if (dataStoreEventListener != null) {
      minUncommittedIndexInMemstores = dataStoreEventListener
        .getMinUnpersistedIndex();
      if (minUncommittedIndexInMemstores == Long.MAX_VALUE) {
        minUncommittedIndexInMemstores = -1;
      }
    }
    return Math.min(peerManager.getMinUnPersistedIndexAcrossQuorum(),
      minUncommittedIndexInMemstores - 1);
  }

  @Override public PeerStatus getStatus() {
    return new PeerStatus(getQuorumName(), getRanking(), getCurrentEdit().getTerm(),
      getPaxosState(), getLogState(), "", getStoreState());
  }

  private int getHeartbeatTimeoutInMilliseconds() {
    return Math.round(progressTimeoutForLowestRankMillis / 20);
  }

  @Override
  public int getAppendEntriesMaxTries() {
    return Math.max(1, appendEntriesTimeoutInMilliseconds /
      getHeartbeatTimeoutInMilliseconds());
  }

  @Override
  public void setUpdateMembershipRequest(
    QuorumMembershipChangeRequest request) {
    this.updateMembershipRequest = request;
  }

  @Override
  public PeerManagerInterface getPeerManager() {
    return peerManager;
  }

  @Override
  public String getLeaderNotReadyMsg() {
    return "The current server " + this.getMyAddress() +
      " is not the leader for the quorum " +
      this.getQuorumName();
  }

  public void timeoutNow() {
    LOG.info("Timing out immediately because we are asked to");
    this.offerEvent(new ProgressTimeoutEvent());
  }

  @Override
  public void updatePeerAckedId(String address, EditId remoteEdit) {
    peerManager.updatePeerAckedId(address, remoteEdit);
  }

  @Override
  public final RaftEventListener getDataStoreEventListener() {
    return dataStoreEventListener;
  }

  /**
   * Returns the min across all the persisted index across peers in the quorum,
   * and the locally flushed edits.
   */
  @Override
  public long getMinUnPersistedIndexAcrossQuorum() {
    return peerManager.getMinUnPersistedIndexAcrossQuorum();
  }

  @Override
  public void setMinAckedIndexAcrossAllPeers(long index) {
    peerManager.setMinAckedIndexAcrossAllPeers(index);
  }

  public void reseedIndex(long seqid)
    throws ExecutionException, InterruptedException, IOException {
    // If it is initialized go through the state machine to avoid race
    // conditions, else directly update it.
    if (stateMachine != null) {
      ReseedRequest request = new ReseedRequest(seqid);
      // Create replicate event and add the event to the state machine
      ReseedRequestEvent event = new ReseedRequestEvent(request);

      // Get the future object and wait for the result
      ListenableFuture<Boolean> futureResult = event.getRequest().getResult();
      if (!offerEvent(event)) {
        event.getRequest().getResult().setException(new Exception("Cannot" +
          "offer the event to the state machine."));
      }
      futureResult.get();
    } else {
      reseedStartIndex(seqid);
    }
  }

  @Override
  public void setQuorumInfo(final QuorumInfo quorumInfo) {
    this.quorumInfo = quorumInfo;
    // Update the rank if needed, making sure this replica is still part of the
    // set before doing so.
    int expectedRank = quorumInfo.getRank(regionServerAddress);
    if (expectedRank != rank && expectedRank > 0) {
      LOG.info("Updating my quorumInfo, old rank: " + getRanking() +
        ", new rank: " + expectedRank);
      initializeRank();
      progressTimeoutForLowestRankMillis = conf.getInt(
              HConstants.PROGRESS_TIMEOUT_INTERVAL_KEY,
              HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS);
      progressTimer.setDelay(getProgressTimeoutForMeMillis(),
        TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void setLastAppendRequestReceivedTime(long timeMillis) {
    this.lastAppendRequestTimeMillis = timeMillis;
  }

  @Override
  public long getLastAppendRequestReceivedTime() {
    return lastAppendRequestTimeMillis;
  }

  @Override
  public int getNumPendingEvents() {
    return stateMachine.getNumPendingEvents();
  }

  @Override
  public boolean isPartOfNewQuorum() {
    return partOfNewQuorum;
  }

  /**
   * Returns the progress timeout for the curren replica.
   * @return timeout values in ms.
   */
  public long getProgressTimeoutForMeMillis() {
    if (this.canTakeOver) {
      return Math.round(
        progressTimeoutForLowestRankMillis * 1.0 / rank);
    } else {
      return progressTimeoutForLowestRankMillis;
    }
  }

  public FiniteStateMachineService createFiniteStateMachineService(FiniteStateMachine fsm) {
    return new ConstitutentFSMService(fsm, serialExecutor.createStream());
  }

  @Override
  public ExecutorService getExecServiceForThriftClients() {
    return execServiceForThriftClients;
  }

  private void initializeMetaData() {
    String consensusMetadataDirectoryStr = conf.get(
      HConstants.RAFT_METADATA_DIRECTORY_KEY,
      HConstants.DEFAULT_RAFT_METADATA_DIRECTORY
    );

    if (!consensusMetadataDirectoryStr.endsWith(HConstants.PATH_SEPARATOR)) {
      consensusMetadataDirectoryStr =
        consensusMetadataDirectoryStr + HConstants.PATH_SEPARATOR;
    }
    consensusMetadataDirectoryStr +=
      HConstants.PATH_SEPARATOR + getQuorumName() + HConstants.PATH_SEPARATOR;

    lastVotedForFilePath = Paths.get(
      consensusMetadataDirectoryStr + "lastVotedFor");

    try {
      // Create the consensus meta-data directory, as it might have not
      // existed, causing this exception. If the directory already exists,
      // there will not be any exception.
      Files.createDirectories(Paths.get(consensusMetadataDirectoryStr));
      lastVotedForFile =
        new RandomAccessFile(lastVotedForFilePath.toString(), "rws");
    } catch (FileNotFoundException fnf) {
      // This exception should not happen if the directory is created
      // successfully, because we use the "rw" option, which creates the file
      // if it does not exist.
      LOG.error("Could not open metadata files: ", fnf);
    } catch (IOException ioe) {
      LOG.error("Could not initialize metadata: ", ioe);
    }

    try {
      int fileLength = (int)lastVotedForFile.length();
      if (fileLength == 0) {
        lastVotedFor = null;
        return;
      }

      byte[] lastVotedForSerialized = new byte[fileLength];
      lastVotedForFile.readFully(lastVotedForSerialized);
      lastVotedFor = Bytes.readThriftBytes(lastVotedForSerialized,
        ConsensusHost.class, TCompactProtocol.class);
    } catch (Exception e) {
      LOG.info("An error while reading the metadata file: ", e);
    }
  }

  private void persistLastVotedFor() {
    // This case would never happen, unless the file was not opened
    // successfully, which is also unlikely (see the initializeMetaData()
    // method). In this case, its better to not persist lastVotedFor, than
    // get an NPE.
    if (lastVotedForFile == null) {
      throw new IllegalStateException("lastVotedFor file was not created");
    }

    if (lastVotedFor == null) {
      // If the file exists, we will clear it
      try {
        lastVotedForFile.getChannel().truncate(0);
      } catch (IOException ioe) {
        LOG.error("Could not write to the lastVotedFor file", ioe);
        return;
      }
      return;
    }

    try {
      ByteBuffer lastVotedForSerialized =
        Bytes.writeThriftBytes(lastVotedFor, ConsensusHost.class,
          TCompactProtocol.class);
      lastVotedForFile.write(Bytes.getBytes(lastVotedForSerialized));
    } catch (Exception e) {
      LOG.error("Could not write to the lastVotedFor file ", e);
    }
  }

  private void initializeStateMachine() {
    if (stateMachine == null) {
      FiniteStateMachine fsm = new RaftStateMachine(
        "RaftFSM-" + getQuorumName() + "-Rank:" +
          getRanking() + "-" + getMyAddress(), this);
      FiniteStateMachineService fsmService;
      if (conf.getBoolean(HConstants.USE_FSMMUX_FOR_RSM,
        HConstants.USE_FSMMUX_FOR_RSM_DEFAULT)) {
        fsmService = createFiniteStateMachineService(fsm);
      } else {
        fsmService = new FiniteStateMachineServiceImpl(fsm);
      }
      setStateMachineService(fsmService);
    }
    stateMachine.offer(new Event(RaftEventType.START));
  }

  private void initializeRank() {
    rank = quorumInfo.getRank(regionServerAddress);
    // Whenever we set the rank, update the thread name, because debugging
    // will be hard if there is a quorum membership change.
    Thread.currentThread()
      .setName("RaftFSM-" + getQuorumName() + "-Rank:" +
        rank + "-" + getMyAddress());
    assert rank > 0 ;
  }

  private void refreshLogState() {
    previousLogEdit = logManager.getLastEditID();
    currentEdit = previousLogEdit.clone();
    committedEdit = logManager.getLastValidTransactionId();
  }

  /**
   * Initiates the replication of the given transaction.
   *
   * @param event
   * @return
   */
  private ListenableFuture<?> createAndSendAppendRequest(ReplicateEntriesEvent event) {

    // 1. Create a new Edit Id if its not a heartbeat
    if (!event.isHeartBeat()) {
      // Create a new appendEditId for a non heartbeat msg
      currentEdit = EditId.getNewAppendEditID(currentEdit);
      // Update the previous and the current edit id
    }

    // 2. Create an append request
    final AppendRequest request =
      AppendRequest.createSingleAppendRequest(quorumInfo.getQuorumName(),
        leader, currentEdit, previousLogEdit, committedEdit.getIndex(),
        getMinUnPersistedIndexAcrossQuorum(), event.isHeartBeat(), event.getEntries());

    // 3. Create a new session for this append request
    if (LOG.isTraceEnabled()) {
      LOG.trace("creating an AppendConsensusSession from " + request);
    }

    final AppendConsensusSessionInterface appendSession =
      peerManager.createAppendConsensusSession(majorityCnt, request, event,
        metrics, rank, safeToStepDown());

    outstandingAppendSession = appendSession;

    // 4. Send the request to peers
    peerManager.sendAppendRequestToQuorum(request);

    // 5. If this is not a hearbeat, then execute the appendToLog in a separate
    // thread pool.
    if (!event.isHeartBeat()) {
      previousLogEdit = currentEdit;
      return FSMLargeOpsExecutorService.submitToWriteOpsThreadPool(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          appendToLog(request.getLogId(0), committedEdit.getIndex(),
            request.getEdit(0));

          if (isLogAccessible()) {
            // Ack yourself if append succeeded
            appendSession.incrementAck(currentEdit, myAddress, rank, false);
          }
          return null;
        }
      });
    } else {
      if (isLogAccessible()) {
        appendSession.incrementAck(currentEdit, myAddress, rank, false);
      }
      return null;
    }
  }

  /**
   * Tells whether its safe to step down from leadership or not.
   * @return
   */
  private boolean safeToStepDown() {
    if (dataStoreEventListener == null) {
      return true;
    }

    boolean bool = enableStepdownOnHigherRankConf &&
      this.dataStoreEventListener.canStepDown();
    if (!bool) {
      LOG.debug("Not ready to step down now");
    }
    return bool;
  }
}
