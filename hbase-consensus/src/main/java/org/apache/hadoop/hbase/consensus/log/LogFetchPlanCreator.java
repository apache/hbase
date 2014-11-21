package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.consensus.client.FetchTask;

import java.util.Collection;

/**
 * Interface for creating a list of FetchTasks. Implementation classes
 * contain algorithms to make decision based on given information.
 */
public interface LogFetchPlanCreator {
  Collection<FetchTask> createFetchTasks();
}
