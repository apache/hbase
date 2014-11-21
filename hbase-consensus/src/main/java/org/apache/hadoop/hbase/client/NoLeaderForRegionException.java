package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.RegionException;

/**
 * Thrown when the quorum client cannot find the leader for a particular
 * region.
 */
public class NoLeaderForRegionException extends RegionException {
  /** default constructor */
  public NoLeaderForRegionException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public NoLeaderForRegionException(String s) {
    super(s);
  }
}
