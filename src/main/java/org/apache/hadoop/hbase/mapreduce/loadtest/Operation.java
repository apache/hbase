package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.loadtest.StatsCollector.Stats;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Represents an action that can be performed against an HBase HTable. The HBase
 * configuration and table are defined statically. The Operation class maintains
 * thread-local references to HTables, allowing multiple concurrent operations
 * to be performed against the same table. Subclasses of Operation are not
 * required to be thread-safe, and should not be accessed from multiple threads
 * concurrently.
 */
public abstract class Operation implements Runnable {

  private static volatile HBaseConfiguration conf;
  private static volatile byte[] tableName;

  /**
   * HTable instances are not thread-safe, so they need to be encapsulated in a
   * ThreadLocal variable.
   */
  private static final ThreadLocal<HTable> table = new ThreadLocal<HTable>() {
    @Override
    protected HTable initialValue() {
      try {
        return new HTable(conf, tableName);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }
  };

  /**
   * Initialize the HBase configuration and table name against which all
   * operations will be performed. This must be done before any operation is
   * executed.
   *
   * @param conf the HBase configuration to use
   * @param tableName the name of the HBase table against which to operate
   */
  public static void setConfAndTableName(
      HBaseConfiguration conf, byte[] tableName) {
    Operation.conf = conf;
    Operation.tableName = tableName;
  }

  /**
   * The type of operation, used for reporting and statistics.
   */
  public enum Type {
    GET("get", "R"),
    BULK_PUT("bulk-put", "W"),
    PUT("put", "W");

    public final String name;
    public final String shortName;

    private Type(String name, String shortName) {
      this.name = name;
      this.shortName = shortName;
    }
  };

  /**
   * Get the type of the operation represented by the subclass.
   * @return the type of the operation
   */
  public abstract Type getType();

  /**
   * Perform this operation against a specific HBase table.
   *
   * @param table HBase table against which to perform the operation
   * @throws IOException
   */
  public abstract void perform(HTable table) throws IOException;

  /**
   * Do any cleanup or verification required after the operation is performed.
   * This will be called after the operation is performed, regardless of whether
   * or not the operation was successful or threw an exception.
   */
  public abstract void postAction();

  /**
   * Get the number of keys affected by this operation. For some operations,
   * which may be part of a group of operations acting on a single key, this
   * method may return 0 for all but the last part to complete.
   *
   * @return the number of keys affected by this operation
   */
  public abstract long getNumKeys();

  /**
   * Get the number of columns affected by this operation.
   *
   * @return the number of columns affected by this operation
   */
  public abstract long getNumColumns();

  /**
   * Performs this operation against the table.
   */
  public void run() {
    StatsCollector.Stats stats =
        StatsCollector.getInstance().getStats(this.getType());
    long start = System.currentTimeMillis();

    try {
      perform(table.get());
    } catch (IOException e) {
      stats.incrementFailures(1);
    } catch (Exception e) {
      e.printStackTrace();
      stats.incrementFailures(1);
    } finally {
      long elapsed = System.currentTimeMillis() - start;
      stats.incrementCumulativeOpTime(elapsed);
      stats.incrementKeys(getNumKeys());
      stats.incrementColumns(getNumColumns());
      postAction();
    }
  }
}
