package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.IOException;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RegionSplitter;

/**
 * A workload encapsulates the sequence of operations to be executed (as
 * represented by an OperationGenerator instance) as well as constraints on how
 * those operations are to be executed, including the size of the thread pool
 * and the target number of operations per second.
 */
public interface Workload extends Serializable {

  /**
   * Construct a generator which provides the operations of this workload. This
   * method will return a new generator with each invocation, so its output
   * should be cached and reused.
   *
   * @return a generator for this workload
   */
  public OperationGenerator constructGenerator();

  /**
   * Get the configured number of threads required to execute this workload.
   *
   * @return the number of threads required to execute this workload
   */
  public int getNumThreads();

  /**
   * Get the desired number of operations per second for this workload.
   *
   * @return the desired number of operations per second
   */
  public int getOpsPerSecond();

  /**
   * Get the types of operations used by this workload, so other resources can
   * be properly initialized.
   *
   * @return set of operation types use by this workload
   */
  public EnumSet<Operation.Type> getOperationTypes();

  /**
   * Workload Generators are responsible for constructing the workloads that
   * will be run by each of the client nodes. Since different workloads may have
   * different requirements for how workloads are distributed across clients,
   * that logic is encapsulated in workload generator implementations.
   */
  public static abstract class Generator {

    /**
     * Generate the workloads for a certain number of clients nodes. Each client
     * may be assigned multiple workloads, to isolate different aspects of a
     * workload, since each Workload instance will be executed with its own
     * threadpool. Any parameters provided for generating the workloads will be
     * passed as a single String, with implementation-specific formatting.
     *
     * @param numWorkloads the number of clients for which to generate workloads
     * @param args the parameters for generating the workloads
     * @return a list of lists of workloads, one for each client node
     */
    public abstract List<List<Workload>> generateWorkloads(
        int numWorkloads, String args);

    /**
     * Helper method which deletes a particular table if it exists, then creates
     * a new table with that name according to the implementation.
     *
     * @param conf configuration used to access the HBase cluster
     * @param tableName the name of the table to setup
     * @throws IOException
     * @throws MasterNotRunningException
     */
    public void setupTable(HBaseConfiguration conf, String tableName)
        throws IOException, MasterNotRunningException {
      // Need to modify the configuration to have a higher timeout for RPCs
      // because creating a presplit table with many regions might take a while.
      @SuppressWarnings("deprecation")
      HBaseConfiguration newConf = new HBaseConfiguration(conf);
      newConf.setInt("hbase.rpc.timeout", 5 * 60000);

      HBaseAdmin admin = new HBaseAdmin(newConf);
      for (HTableDescriptor desc : admin.listTables()){
        if (desc.getNameAsString().equals(tableName)) {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
          break;
        }
      }
      HTableDescriptor desc = getTableDescriptor();
      desc.setName(tableName.getBytes());
      byte[][] splitKeys = getSplitKeys(admin);
      admin.createTable(desc, splitKeys);
    }

    /**
     * Get a descriptor of the table used by this workload. Each workload must
     * define the column families used by that workload, including any
     * non-default parameters.
     *
     * @return a descriptor with the necessary column families defined
     */
    public abstract HTableDescriptor getTableDescriptor();

    /**
     * Helper method to instantiate a Workload Generator instance for a named
     * implementation.
     *
     * @param className the name of the Generator implementation class
     * @return a new instance of the named Generator implementation
     */
    public static Generator newInstance(String className) {
      try {
        return (Generator)Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static final int DEFAULT_REGIONS_PER_SERVER = 5;

    private static byte[][] getSplitKeys(HBaseAdmin admin) throws IOException {
      int numberOfServers = admin.getClusterStatus().getServers();
      int totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
      return new RegionSplitter.HexStringSplit().split(totalNumberOfRegions);
    }
  }

  /**
   * The Executor will execute a workload according to its configuration of
   * threads and rate limit (operations per second), using a specified data
   * generator. Once an executor is started, it will continue until the workload
   * has been exhausted.
   *
   * The Executor uses a ScheduledThreadPoolExecutor to delay the execution of
   * operations to a desired rate of operations per second. However, if there is
   * a delay in the execution of operations and the desired rate is not
   * maintained, then future operations will not be delayed until the overall
   * average rate is equal to the desired rate. This may cause the rate of
   * execution to sometimes exceed the desired rate.
   */
  public static class Executor {

    private static final int EXECUTOR_QUEUEING_FACTOR = 10;
    private static final double EXECUTOR_BUFFER_THREAD_FACTOR = 0.1;

    private final OperationGenerator opGenerator;
    private final DataGenerator dataGenerator;
    private final BoundedExecutor executor;
    private final Workload workload;
    private ScheduledThreadPoolExecutor scheduler;

    public Executor(Workload workload, DataGenerator dataGenerator) {
      this.workload = workload;
      this.dataGenerator = dataGenerator;

      opGenerator = workload.constructGenerator();

      java.util.concurrent.Executor threadPool =
          new ThreadPoolExecutor(workload.getNumThreads(),
              workload.getNumThreads(), Long.MAX_VALUE, TimeUnit.NANOSECONDS,
              new ArrayBlockingQueue<Runnable>(
                  EXECUTOR_QUEUEING_FACTOR * workload.getNumThreads()));

      executor = new BoundedExecutor(threadPool,
          EXECUTOR_QUEUEING_FACTOR * workload.getNumThreads());
    }

    /**
     * Start executing the workload.
     */
    public void start() {
      if (workload.getOpsPerSecond() <= 0) {
        throw new IllegalArgumentException("Workload must have positive " +
            "number of operations per second");
      }

      int numThreads = (int) (Math.ceil(workload.getNumThreads() *
          EXECUTOR_BUFFER_THREAD_FACTOR));

      // Determine the delay in nanoseconds between operations for each thread.
      long delayNS = numThreads * 1000000000L / workload.getOpsPerSecond();

      scheduler = new ScheduledThreadPoolExecutor(numThreads);
      final Runnable queueNextOperation = new Runnable() {
        public void run() {
          try {
            while (true) {
              Operation operation = opGenerator.nextOperation(dataGenerator);
              if (operation != null) {
                executor.submitTask(operation);
                return;
              }
            }
          } catch (InterruptedException e) {
          } catch (OperationGenerator.ExhaustedException e) {
            // By default, ScheduledThreadPoolExecutor cancels periodic tasks
            // when shutdown() is called, so shutdownNow() should not be needed.
            scheduler.shutdown();
          }
        }
      };

      // If a task runs longer than the period, subsequent tasks will start
      // late. Since generating operations takes some amount of time, have
      // multiple threads for generating and submitting operations.
      for (int i = 0; i < numThreads; i++) {
        scheduler.scheduleAtFixedRate(queueNextOperation, 0, delayNS,
            TimeUnit.NANOSECONDS);
      }
    }

    /**
     * Wait for the executor to finish, which happens when its workload has
     * been exhausted. This must be called after start().
     */
    public void waitForFinish() {
      if (scheduler == null) {
        throw new IllegalStateException("Executor has not started yet");
      }
      try {
        scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    /**
     * A wrapper for an executor which blocks threads attempting to submit tasks
     * when a certain bound of tasks are already queued or executing.
     */
    private static class BoundedExecutor {
      private final java.util.concurrent.Executor exec;
      private final Semaphore semaphore;

      public BoundedExecutor(java.util.concurrent.Executor exec, int bound) {
          this.exec = exec;
          this.semaphore = new Semaphore(bound);
      }

      public void submitTask(final Runnable command)
              throws InterruptedException {
          semaphore.acquire();
          try {
              exec.execute(new Runnable() {
                  public void run() {
                      try {
                          command.run();
                      } finally {
                          semaphore.release();
                      }
                  }
              });
          } catch (RejectedExecutionException e) {
              semaphore.release();
          }
      }
    }
  }
}
