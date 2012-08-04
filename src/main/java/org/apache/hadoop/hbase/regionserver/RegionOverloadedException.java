package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.RegionException;

public class RegionOverloadedException extends RegionException {
  private static final long serialVersionUID = -8436877560512061623L;

  long backOffTime;


  /** default constructor */
  public RegionOverloadedException() {
    super();
  }

  /** @param s message
   *  @param waitMillis -- request client to backoff for waitMillis
   */
  public RegionOverloadedException(String s, long waitMillis) {
    super(s);
    backOffTime = waitMillis;
  }

  public long getBackoffTimeMillis() {
    return backOffTime;
  }

}
