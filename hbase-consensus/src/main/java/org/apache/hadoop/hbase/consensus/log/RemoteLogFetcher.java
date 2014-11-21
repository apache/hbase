package org.apache.hadoop.hbase.consensus.log;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.consensus.client.FetchTask;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Generate fetch plan from local quorum context.
 * Also control the actual fetch work.
 */
public class RemoteLogFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteLogFetcher.class);

  private RaftQuorumContext context;
  QuorumClient quorumClient;

  public RemoteLogFetcher() {}

  public RemoteLogFetcher(RaftQuorumContext context) {
    this.context = context;
  }

  /**
   * Create a fetch plan from local context and download log files.
   */
  public void doSpontaneousFetch() throws Exception {
    initializeQuorumClients();
    List<Pair<String, List<LogFileInfo>>> statuses =
      getCommittedLogInfoFromAllPeers();
    Collection<FetchTask> tasks = createFetchTasks(
        statuses, context.getCommittedEdit().getIndex());
    executeTasks(tasks, context.getQuorumName());
  }

  /**
   * Instantiate connections to peers
   */
  public void initializeQuorumClients() throws IOException {
    this.quorumClient =
        new QuorumClient(context.getQuorumInfo(), context.getConf(),
            context.getExecServiceForThriftClients());
  }

  /**
   * Ask peers for information of committed log files which have greater index than
   * the latest local committed index
   *
   * @return each list item contains committed log info of a peer
   */
  public List<Pair<String, List<LogFileInfo>>> getCommittedLogInfoFromAllPeers()
    throws Exception {
    return getPeerCommittedLogStatus(context.getCommittedEdit().getIndex());
  }

  /**
   * Ask peers for information of committed log files which have greater index than
   * a given index. It's only used by tests.
   *
   * @param minIndex
   * @return each list item contains committed log info of a peer
   */
  protected List<Pair<String, List<LogFileInfo>>> getPeerCommittedLogStatus(long minIndex)

    throws Exception {
    List<Pair<String, List<LogFileInfo>>> statuses = quorumClient.getPeerCommittedLogStatus(
        context.getServerAddress(), context.getQuorumName(), minIndex);

    return statuses;
  }

  protected Collection<FetchTask> createFetchTasks(
      List<Pair<String, List<LogFileInfo>>> statuses, long minIndex) {
    LogFetchPlanCreator planCreator = new LogFileInfoIterator(statuses, minIndex);
    return planCreator.createFetchTasks();
  }

  /**
   * Execute fetch tasks either generated locally or pushed from a remote server
   *
   * @param tasks each task item contains the address of one peer and a list of files to
   *              be downloaded from it
   */
  public ListenableFuture<Void> executeTasks(Collection<FetchTask> tasks, String regionId) {
    //TODO to be implemented in part 2
    return null;
  }
}
