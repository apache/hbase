package org.apache.hadoop.hbase.consensus.quorum;

import java.util.concurrent.TimeUnit;

public interface Timer {
  public void start();
  public void stop();
  public void reset();
  public void shutdown();
  public void backoff(long backOffTime, TimeUnit units);
  public void setDelay(long delay, TimeUnit unit);
}
