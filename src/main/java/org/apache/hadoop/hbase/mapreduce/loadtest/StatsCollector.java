package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.lang.management.ManagementFactory;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Collects statistics on the performance of operations. StatsCollector is a
 * singleton which manages all of the statistics for all operations within a
 * client. The StatsCollector also provides reporters which make the collected
 * statistics available through multiple outputs.
 */
public class StatsCollector {

  private static final int CONSOLE_REPORTER_PERIOD_MS = 5000;
  private static final int MRCOUNTER_REPORTER_PERIOD_MS = 5000;

  private ConcurrentHashMap<Operation.Type, Stats> map;
  private static final StatsCollector instance = new StatsCollector();

  /**
   * Get the singleton instance.
   *
   * @return the singleton instance
   */
  public static StatsCollector getInstance() {
    return instance;
  }

  /**
   * Private constructor to enforce singleton access.
   */
  private StatsCollector() {
    this.map = new ConcurrentHashMap<Operation.Type, Stats>();
  }

  /**
   * Get the Stats object for a certain type of operation.
   *
   * @param type
   * @return the Stats object for the specified operation type
   */
  public Stats getStats(Operation.Type type) {
    Stats stats = map.get(type);
    if (stats == null) {
      synchronized (map) {
        // Double-check-locking is safe here because map is thread-safe.
        if (map.get(type) == null) {
          stats = new Stats(type);
          map.put(type, stats);
        }
      }
    }
    return stats;
  }

  /**
   * Ensure that Stats objects for the specified operation types are initialized
   * and will be available to any reporters started after this call.
   *
   * @param types the set of operation types to initialize
   */
  public void initializeTypes(EnumSet<Operation.Type> types) {
    for (Operation.Type type : types) {
      getStats(type);
    }
  }

  /**
   * Start the MBean reporters for each of the initialized types of Stats. This
   * should only be called once, and only after all Stats have been initialized.
   */
  public void startMBeanReporters() {
    for (Stats stats : map.values()) {
      new StatsMBeanReporter(stats);
    }
  }

  /**
   * Start a console reporter for each of the initialized types of Stats. This
   * should only be called once, and only after all Stats have been initialized.
   */
  public void startConsoleReporters() {
    for (Stats stats : map.values()) {
      new StatsConsoleReporter(stats);
    }
  }

  /**
   * Start a mapreduce counter reporter for each of the initialized types of
   * Stats. This should only be called once, and only after all Stats have been
   * initialized.
   *
   * @param context the mapreduce context of which to update the counters
   */
  public void startMapReduceCounterReporter(
      @SuppressWarnings("rawtypes") final TaskInputOutputContext context) {
    for (Stats stats : map.values()) {
      new StatsMapReduceCounterReporter(stats, context);
    }
  }

  /**
   * Represents the statistics collected about a particular type of operation.
   */
  public static class Stats {

    private final Operation.Type type;

    private final AtomicLong numKeys;
    private final AtomicLong numKeysVerified;
    private final AtomicLong numColumns;
    private final AtomicLong numErrors;
    private final AtomicLong numFailures;
    private final AtomicLong cumulativeOpTime;

    private final long startTime;

    public Stats(Operation.Type type) {
      this.type = type;

      this.numKeys = new AtomicLong(0);
      this.numKeysVerified = new AtomicLong(0);
      this.numColumns = new AtomicLong(0);
      this.numErrors = new AtomicLong(0);
      this.numFailures = new AtomicLong(0);
      this.cumulativeOpTime = new AtomicLong(0);

      this.startTime = System.currentTimeMillis();
    }

    public Operation.Type getType() {
      return this.type;
    }

    public long getNumKeys() {
      return numKeys.get();
    }

    public void incrementKeys(long delta) {
      numKeys.addAndGet(delta);
    }

    public long getNumKeysVerified() {
      return numKeysVerified.get();
    }

    public void incrementKeysVerified(long delta) {
      numKeysVerified.addAndGet(delta);
    }

    public long getNumColumns() {
      return numColumns.get();
    }

    public void incrementColumns(long delta) {
      numColumns.addAndGet(delta);
    }

    public long getNumErrors() {
      return numErrors.get();
    }

    public void incrementErrors(long delta) {
      numErrors.addAndGet(delta);
    }

    public long getNumFailures() {
      return numFailures.get();
    }

    public void incrementFailures(long delta) {
      numFailures.addAndGet(delta);
    }

    public long getCumulativeOpTime() {
      return cumulativeOpTime.get();
    }

    public void incrementCumulativeOpTime(long delta) {
      cumulativeOpTime.addAndGet(delta);
    }

    public long getStartTime() {
      return startTime;
    }
  }

  /**
   * A snapshot of an operation's stats at a particular time, to facilitate
   * reporting of the rate of change of certain statistics.
   */
  public static class StatsSnapshot {

    protected final Stats stats;

    protected final AtomicLong priorKeysPerSecondCumulativeKeys;
    protected final AtomicLong priorKeysPerSecondTime;
    protected final AtomicLong priorColumnsPerSecondCumulativeColumns;
    protected final AtomicLong priorColumnsPerSecondTime;
    protected final AtomicLong priorLatencyCumulativeKeys;
    protected final AtomicLong priorLatencyCumulativeLatency;

    public StatsSnapshot(Stats stats) {
      long now = System.currentTimeMillis();
      this.stats = stats;
      this.priorKeysPerSecondCumulativeKeys = new AtomicLong(0);
      this.priorKeysPerSecondTime = new AtomicLong(now);
      this.priorColumnsPerSecondCumulativeColumns = new AtomicLong(0);
      this.priorColumnsPerSecondTime = new AtomicLong(now);
      this.priorLatencyCumulativeKeys = new AtomicLong(0);
      this.priorLatencyCumulativeLatency = new AtomicLong(0);
    }

    public synchronized long getKeysPerSecond() {
      long currentTime = System.currentTimeMillis();
      long priorTime = priorKeysPerSecondTime.getAndSet(currentTime);
      long currentKeys = stats.getNumKeys();
      long priorKeys = priorKeysPerSecondCumulativeKeys.getAndSet(currentKeys);
      long timeDelta = currentTime - priorTime;
      if (timeDelta == 0) {
        return 0;
      }
      return 1000 * (currentKeys - priorKeys) / timeDelta;
    }

    public synchronized long getColumnsPerSecond() {
      long currentTime = System.currentTimeMillis();
      long priorTime = priorColumnsPerSecondTime.getAndSet(currentTime);
      long currentColumns = stats.getNumColumns();
      long priorColumns = priorColumnsPerSecondCumulativeColumns
          .getAndSet(currentColumns);
      long timeDelta = currentTime - priorTime;
      if (timeDelta == 0) {
        return 0;
      }
      return 1000 * (currentColumns - priorColumns) / timeDelta;
    }

    public synchronized long getAverageLatency() {
      long currentLatency = stats.getCumulativeOpTime();
      long priorLatency = priorLatencyCumulativeLatency
          .getAndSet(currentLatency);
      long currentKeys = stats.getNumKeys();
      long priorKeys = priorLatencyCumulativeKeys.getAndSet(currentKeys);
      long keyDelta = currentKeys - priorKeys;
      if (keyDelta == 0) {
        return 0;
      }
      return (currentLatency - priorLatency) / keyDelta;
    }

    public long getCumulativeKeysPerSecond() {
      long timeDelta = System.currentTimeMillis() - stats.getStartTime();
      if (timeDelta == 0) {
        return 0;
      }
      return 1000 * stats.getNumKeys() / timeDelta;
    }

    public long getCumulativeKeys() {
      return stats.getNumKeys();
    }

    public long getCumulativeColumns() {
      return stats.getNumColumns();
    }

    public long getCumulativeAverageLatency() {
      if (stats.getNumKeys() == 0) {
        return 0;
      }
      return stats.getCumulativeOpTime() / stats.getNumKeys();
    }

    public long getCumulativeErrors() {
      return stats.getNumErrors();
    }

    public long getCumulativeOpFailures() {
      return stats.getNumFailures();
    }

    public long getCumulativeKeysVerified() {
      return stats.getNumKeysVerified();
    }
  }

  /**
   * Periodically reports an operation's stats to the standard output console.
   */
  private static class StatsConsoleReporter extends StatsSnapshot {

    public static String formatTime(long elapsedTime) {
      String format = String.format("%%0%dd", 2);
      elapsedTime = elapsedTime / 1000;
      String seconds = String.format(format, elapsedTime % 60);
      String minutes = String.format(format, (elapsedTime % 3600) / 60);
      String hours = String.format(format, elapsedTime / 3600);
      String time =  hours + ":" + minutes + ":" + seconds;
      return time;
    }

    public StatsConsoleReporter(final Stats stats) {
      super(stats);
      final long start = System.currentTimeMillis();

      new Thread(new Runnable() {
        public void run() {
          while (true) {
            System.out.println(formatTime(System.currentTimeMillis() - start) +
                " [" + stats.getType().name + "] " +
                "keys/s: [current: " + getKeysPerSecond() + " average: " +
                getCumulativeKeysPerSecond() +"] " +
                "latency: [current: " + getAverageLatency() + " average: " +
                getCumulativeAverageLatency() + "] " +
                "errors: " + getCumulativeErrors() +
                " failures: " + getCumulativeOpFailures());
            try {
              Thread.sleep(CONSOLE_REPORTER_PERIOD_MS);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }).start();
    }
  }

  /**
   * Periodically reports an operation's stats to mapreduce counters. Only stats
   * which are additive between clients will be reported.
   */
  private static class StatsMapReduceCounterReporter extends StatsSnapshot {
    public StatsMapReduceCounterReporter(final Stats stats,
        @SuppressWarnings("rawtypes") final TaskInputOutputContext context) {
      super(stats);

      new Thread(new Runnable() {
        public void run() {
          String typeName = stats.getType().name;
          long oldCumulativeKeysPerSecond = 0;
          long oldCurrentKeysPerSecond = 0;
          long oldCumulativeKeys = 0;
          long oldCumulativeFailures = 0;
          long oldCumulativeErrors = 0;

          while (true) {
            long newCumulativeKeysPerSecond = getCumulativeKeysPerSecond();
            context.getCounter("cumulativeKeysPerSecond", typeName).increment(
                newCumulativeKeysPerSecond - oldCumulativeKeysPerSecond);
            oldCumulativeKeysPerSecond = newCumulativeKeysPerSecond;

            long newCurrentKeysPerSecond = getKeysPerSecond();
            context.getCounter("currentKeysPerSecond", typeName).increment(
                newCurrentKeysPerSecond - oldCurrentKeysPerSecond);
            oldCurrentKeysPerSecond = newCurrentKeysPerSecond;

            long newCumulativeKeys = stats.getNumKeys();
            context.getCounter("cumulativeKeys", typeName).increment(
                newCumulativeKeys - oldCumulativeKeys);
            oldCumulativeKeys = newCumulativeKeys;

            long newCumulativeFailures = getCumulativeOpFailures();
            context.getCounter("cumulativeFailures", typeName).increment(
                newCumulativeFailures - oldCumulativeFailures);
            oldCumulativeFailures = newCumulativeFailures;

            long newCumulativeErrors = getCumulativeErrors();
            context.getCounter("cumulativeErrors", typeName).increment(
                newCumulativeErrors - oldCumulativeErrors);
            oldCumulativeErrors = newCumulativeErrors;

            context.progress();

            try {
              Thread.sleep(MRCOUNTER_REPORTER_PERIOD_MS);
            } catch(InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }).start();
    }
  }

  /**
   * Make an operation's stats available for query via an MBean. Any rate-based
   * stats will be calculated for the time between successive invocations of the
   * MBean interface.
   */
  private static class StatsMBeanReporter extends StatsSnapshot
      implements StatsMBeanReporterMBean {

    public StatsMBeanReporter(Stats stats) {
      super(stats);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      try {
        ObjectName name = new ObjectName("LoadTester:name=" +
            stats.getType().shortName);
        mbs.registerMBean(this, name);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static interface StatsMBeanReporterMBean {
    /**
     * @return the average number of keys processed per second since the
     *         previous invocation of this method
     */
    public long getKeysPerSecond();

    /**
     * @return the average number of columns processed per second since the
     *         previous invocation of this method
     */
    public long getColumnsPerSecond();

    /**
     * @return the average latency of operations since the previous invocation
     *         of this method
     */
    public long getAverageLatency();

    /**
     * @return the average number of keys processed per second since the
     *         creation of this action
     */
    public long getCumulativeKeysPerSecond();

    /**
     * @return the total number of keys processed since the creation of this
     *         action
     */
    public long getCumulativeKeys();

    /**
     * @return the total number of columns processed since the creation of this
     *         action
     */
    public long getCumulativeColumns();

    /**
     * @return the average latency of operations since the creation of this
     *         action
     */
    public long getCumulativeAverageLatency();

    /**
     * @return the total number of errors since the creation of this action
     */
    public long getCumulativeErrors();

    /**
     * @return the total number of operation failures since the creation of this
     *         action
     */
    public long getCumulativeOpFailures();

    /**
     * @return the total number of keys verified since the creation of this
     *         action
     */
    public long getCumulativeKeysVerified();

  }
}
