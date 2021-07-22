package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The Class MetricsSourceRefresherChore for refreshing source metrics
 */
@InterfaceAudience.Private
public class MetricsReplicationSourceRefresherChore extends ScheduledChore {

	private ReplicationSource replicationSource;

	private MetricsSource metrics;
	
	public static final String DURATION = "hbase.metrics.replication.source.refresher.duration";
	public static final int DEFAULT_DURATION = 60000;

	public MetricsReplicationSourceRefresherChore(Stoppable stopper, ReplicationSource replicationSource) {
		this(DEFAULT_DURATION, stopper, replicationSource);
	}

	public MetricsReplicationSourceRefresherChore(int duration, Stoppable stopper, ReplicationSource replicationSource) {
	    super("MetricsSourceRefresherChore", stopper, duration);
	    this.replicationSource = replicationSource;
	    this.metrics = this.replicationSource.getSourceMetrics();
	}
	
	@Override
	protected void chore() {
		this.metrics.setOldestWalAge(getOldestWalAge());
	}

	/*
    Returns the age of oldest wal.
    */
	long getOldestWalAge() {
	    long now = EnvironmentEdgeManager.currentTime();
	    long timestamp = getOldestWalTimestamp();
	    if (timestamp == Long.MAX_VALUE) {
	      // If there are no wals in the queue then set the oldest wal timestamp to current time
	      // so that the oldest wal age will be 0.
	      timestamp = now;
	    }
	    long age = now - timestamp;
	    return age;
	}
	/*
	  Get the oldest wal timestamp from all the queues.
	*/
	private long getOldestWalTimestamp() {
		long oldestWalTimestamp = Long.MAX_VALUE;
		for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : this.replicationSource.getQueues().entrySet()) {
			PriorityBlockingQueue<Path> queue = entry.getValue();
			Path path = queue.peek();
			// Can path ever be null ?
			if (path != null) {
				oldestWalTimestamp = Math.min(oldestWalTimestamp,
						AbstractFSWALProvider.WALStartTimeComparator.getTS(path));
			}
		}
		return oldestWalTimestamp;
	}
}
