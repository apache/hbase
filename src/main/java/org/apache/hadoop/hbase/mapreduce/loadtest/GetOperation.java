package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
 * An operation which reads a row from a table. The result of the operation is
 * held internally and might be verified.
 */
public class GetOperation extends Operation{

  private final long key;
  private final Get get;
  private Result result;
  private DataGenerator dataGenerator;
  private double profilingFraction;
  private Random random;

  public Operation.Type getType() {
    return Operation.Type.GET;
  }

  /**
   * Construct a new get operation.
   *
   * @param key the key to get
   * @param get the populated get object
   * @param dataGenerator the DataGenerator to use to verify the result, or null
   *        if the result should not be verified
   */
  public GetOperation(long key, Get get, DataGenerator dataGenerator,
      double profilingFraction) {
    this.key = key;
    this.get = get;
    this.result = null;
    this.dataGenerator = dataGenerator;
    this.profilingFraction = profilingFraction;
    this.random = new Random ();
  }

  public void perform(HTable table) throws IOException {
    table.setProfiling(random.nextDouble() < profilingFraction);
    result = table.get(get);
  }

  public void postAction() {
    if (dataGenerator != null) {
      if (!dataGenerator.verify(result)) {
        StatsCollector.getInstance().getStats(getType()).incrementErrors(1);
        System.err.println("Verification failed for key " + key);
      }
    }
  }

  public long getNumKeys() {
    return 1;
  }

  public long getNumColumns() {
    return result == null ? 0 : result.size();
  }
}
