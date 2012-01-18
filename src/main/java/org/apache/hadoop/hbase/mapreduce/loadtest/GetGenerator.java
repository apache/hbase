package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.Get;

/**
 * Generates get operations for keys randomly chosen from those which have been
 * marked as successfully completed. Typically this will be used with a
 * KeyCounter that is shared by a generator of insert operations, to read keys
 * which are known to have been inserted.
 */
public class GetGenerator implements OperationGenerator {

  private KeyCounter keysWritten;
  private byte[] columnFamily;
  private Random random;
  private double verifyFraction;
  private int maxVersions;
  private long minTime;
  private long maxTime;
  private long timeDelta;

  /**
   * Construct a generator for get operations which return a single version from
   * any time range.
   *
   * @param columnFamily
   * @param keysWritten the source of keys
   * @param verifyFraction the fraction in [0,1] of operations to verify
   */
  public GetGenerator(byte[] columnFamily, KeyCounter keysWritten,
      double verifyFraction) {
    this(columnFamily, keysWritten, verifyFraction, 1, 0, Long.MAX_VALUE);
  }

  /**
   * Construct a generator for get operations which should return at most a
   * specified number of versions, each of which must be at most a specified
   * age.
   *
   * @param columnFamily
   * @param keysWritten the source of keys
   * @param verifyFraction the fraction in [0,1] of operations to verify
   * @param maxVersions the maximum number of versions to return
   * @param timeDelta the maximum allowed age of a version, in milliseconds
   */
  public GetGenerator(byte[] columnFamily, KeyCounter keysWritten,
      double verifyFraction, int maxVersions, long timeDelta) {
    this(columnFamily, keysWritten, verifyFraction, maxVersions, 0, 0,
        timeDelta);
  }

  /**
   * Construct a generator for get operations which should return at most a
   * specified number of versions, each of which must fall within a specified
   * range of absolute timestamps.
   *
   * @param columnFamily
   * @param keysWritten the source of keys
   * @param verifyFraction the fraction in [0,1] of operations to verify
   * @param maxVersions the maximum number of versions to return
   * @param minTime the earliest allowable timestamp on a version
   * @param maxTime the latest allowable timestamp on a version
   */
  public GetGenerator(byte[] columnFamily, KeyCounter keysWritten,
      double verifyFraction, int maxVersions, long minTime, long maxTime) {
    this(columnFamily, keysWritten, verifyFraction, maxVersions, minTime,
        maxTime, 0);
  }

  /**
   * Private constructor, used by public constructors to set all properties.
   */
  private GetGenerator(byte[] columnFamily, KeyCounter keysWritten,
      double verifyFraction, int maxVersions, long minTime, long maxTime,
      long timeDelta) {
    this.keysWritten = keysWritten;
    this.columnFamily = columnFamily;
    this.random = new Random();
    this.verifyFraction = verifyFraction;
    this.maxVersions = maxVersions;
    this.minTime = minTime;
    this.maxTime = maxTime;
    this.timeDelta = timeDelta;
  }

  /**
   * Get the next operation. This may return null if no successful key could be
   * found.
   *
   * @return the next get operation for a successful key, or null if none found
   */
  public Operation nextOperation(DataGenerator dataGenerator) {
    try {
      long key = keysWritten.getRandomKey();

      Get get = dataGenerator.constructGet(key, columnFamily);
      try {
        get.setMaxVersions(maxVersions);
        if (timeDelta > 0) {
          long currentTime = System.currentTimeMillis();
          get.setTimeRange(currentTime - timeDelta, currentTime);
        } else {
          get.setTimeRange(minTime, maxTime);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      boolean verify = random.nextDouble() < verifyFraction;
      return new GetOperation(key, get, verify ? dataGenerator : null);
    } catch (KeyCounter.NoKeysException e) {
      return null;
    }
  }
}
