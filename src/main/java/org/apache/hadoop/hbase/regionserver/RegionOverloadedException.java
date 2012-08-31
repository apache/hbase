package org.apache.hadoop.hbase.regionserver;

import java.util.List;

import org.apache.hadoop.hbase.RegionException;

public class RegionOverloadedException extends RegionException {
  private static final long serialVersionUID = -8436877560512061623L;

  /** default constructor */
  public RegionOverloadedException() {
    super();
  }

  /** @param s message
   *  @param waitMillis -- request client to backoff for waitMillis
   */
  public RegionOverloadedException(String s, long waitMillis) {
    super(s, waitMillis);
  }

  /**
   * Create a RegionOverloadedException from another one, attaching a set of related exceptions
   * from a batch operation. The new exception reuses the original exception's stack trace.
   *  
   * @param roe the original exception
   * @param exceptions other exceptions that happened in the same batch operation
   * @param waitMillis remaining time for the client to wait in milliseconds
   * @return the new exception with complete information
   */
  public static RegionOverloadedException create(RegionOverloadedException roe,
      List<Throwable> exceptions, int waitMillis) {
    StringBuilder sb = new StringBuilder(roe.getMessage());
    for (Throwable t : exceptions) {
      if (t != roe) {
        sb.append(t.toString());
        sb.append("\n");
      }
    }
    RegionOverloadedException e = new RegionOverloadedException(sb.toString(), waitMillis);
    if (roe != null) {  // Safety check
      e.setStackTrace(roe.getStackTrace());
    }
    return e;
  }

}
