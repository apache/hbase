package org.apache.hadoop.hbase.metrics;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import io.airlift.stats.ExponentialDecay;
import io.airlift.stats.TimeDistribution;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TimeStat extends TimeDistribution {
  private final Ticker ticker;

  public TimeStat(TimeUnit unit) {
    this(ExponentialDecay.oneMinute(), unit);
  }

  public TimeStat(double alpha, TimeUnit unit) {
    this(alpha, unit, Ticker.systemTicker());
  }

  public TimeStat(double alpha, TimeUnit unit, Ticker ticker) {
    super(alpha, unit);
    this.ticker = ticker;
  }

  public synchronized void add(long value, TimeUnit unit) {
    add(unit.toNanos(value));
  }

  public BlockTimer time() {
    return new BlockTimer();
  }

  public class BlockTimer implements AutoCloseable {
    private final long start = ticker.read();

    @Override
    public void close() {
      add(ticker.read() - start);
    }
  }

  public static void main(String [] args) {
    final TimeStat stat = new TimeStat(TimeUnit.MICROSECONDS);
    for (int i = 0; i < 100; ++i) {
      benchmark(stat);
    }
  }

  private static void benchmark(final TimeStat stat) {
    long n = 1000000;
    long[] randomValues = new long[1000];
    Stopwatch stopwatch = new Stopwatch();
    //Stopwatch.createUnstarted();

    long elapsedTotal = 0;
    long cycleMax = 0;
    for (int i = 0; i < n / randomValues.length; ++i) {
      generateValues(randomValues, 3000000L, 4000000000L);
      stopwatch.start();
      for (int j = 0; j < randomValues.length; ++j) {
        stat.add(randomValues[j]);
      }

      long elapsed = stopwatch.elapsedTime(TimeUnit.NANOSECONDS);
      elapsedTotal += elapsed;
      if (elapsed > cycleMax) {
        cycleMax = elapsed;
      }

      stopwatch.reset();
    }
    System.out.printf("Elapsed: %dns, max cycle: %dns\n", elapsedTotal,
            cycleMax);
  }

  private static void generateValues(final long[]a, long start, long end) {
    Preconditions.checkArgument(start < end, "Start should be less than end");
    long delta = end - start;
    Random random = new Random();

    for (int i = 0; i < a.length; ++i) {
      a[i] = (random.nextLong() % delta) + start;
    }
  }
}
