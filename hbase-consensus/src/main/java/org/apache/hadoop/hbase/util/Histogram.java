/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The Histogram class provides a mechanism to sample data points and perform
 * estimates about percentile metrics.
 * Percentile metric is defined as the follows :
 *  A P99 value is the 99th percentile value among the given data points.
 *
 * Usage :
 * Refer to RegionServerMetrics to see how this Histogram can be used to find
 * percentile estimates.
 *
 * The general expected workflow of a Histogram class is as follows:
 * [<Initialize Histogram> [[<addValue>]* [<getPercentileEstimate>]+ <refresh>]*]
 *
 * In the above sequence addValue can be called from different threads, but
 * getPercentileEstimate and refresh should be called from the same thread since
 * they are not mutually thread safe.
 */

public class Histogram {
  private List<Bucket> buckets;
  private int numBuckets;
  private double minValue;
  private double maxValue;

  public static class Stats {
    public double min = 0;
    public double max = 0;
    public double avg = 0;
    public double count = 0;
  }

  private List<Double> underloadSampleList; // We serve the under loaded cases
  // from this list.
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private boolean underload = true;
  public static final Log LOG = LogFactory.getLog(Histogram.class.getName());

  // We will consider the case when we have more than 100 samples as
  // overloaded case and use the Histogram only in such scenarios. In other
  // cases, we can serve the percentile queries using the underloadSampleList
  private static final int DEFAULT_MINIMUM_LOAD = 100;

  /**
   * Create a histogram with the default values of number of buckets,
   * and min/max for the values.
   */
  public Histogram() {
    this(PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
          PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
          PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);
  }

  // Bucket indexing is from 1 to N
  public Histogram(int numBuckets, double lowBound, double maxValue) {
    if (numBuckets < 1 || lowBound >= maxValue) {
      throw new UnsupportedOperationException();
    }
    buckets = new ArrayList<Bucket>(numBuckets);
    underloadSampleList = Collections.synchronizedList(new ArrayList<Double>());
    refresh(numBuckets, lowBound, maxValue);
  }

  public Stats getBasicStats() {
    Stats stats = new Stats();

    this.lock.writeLock().lock();
    try {
      if (underloadSampleList.size() == 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Too few data points. Consider increasing the sampling time.");
        }
        return stats;
      } else if (underload) {
        Collections.sort(underloadSampleList);
        stats.min = underloadSampleList.get(0);
        stats.max = underloadSampleList.get(underloadSampleList.size() - 1);
        stats.count = underloadSampleList.size();
        double sum = 0;
        for (double d : underloadSampleList) {
          sum += d;
        }
        stats.avg = sum / stats.count;

        return stats;
      } else {
        int totalCount = 0;
        double totalSum = 0;
        boolean minFound = false;
        stats.max = Double.MIN_VALUE;

        for (int i = 0; i < this.buckets.size(); i++) {
          Bucket b = this.buckets.get(i);
          if (b.count > 0) {
            if (!minFound) {
              stats.min = b.getMinValue();
              minFound = true;
            }
            stats.max = Math.max(stats.max, b.getMaxValue());
            totalCount += b.getCount();
            totalSum += b.getSum();
          }
        }
        stats.count = totalCount;
        stats.avg = (totalCount == 0) ? 0 : (totalSum / (double) totalCount);
      }
    } catch(Exception e) {
      LOG.error("Unknown Exception : " + e.getMessage());
    } finally {
      this.lock.writeLock().unlock();
    }

    return stats;
  }

  // This is included in the bucket
  private double getBucketStartValue(int bucketIndex) {
    if (bucketIndex < 1 || bucketIndex > this.numBuckets) {
      throw new IndexOutOfBoundsException();
    }
    double gap = this.maxValue - this.minValue;
    double slice = gap/this.numBuckets;
    return this.minValue + (bucketIndex - 1.0)*slice;
  }

  //This is not included in the bucket
  private double getBucketEndValue(int bucketIndex) {
    if (bucketIndex < 1 || bucketIndex > this.numBuckets) {
      throw new IndexOutOfBoundsException();
    }
    double gap = this.maxValue - this.minValue;
    double slice = gap/this.numBuckets;
    return this.minValue + (bucketIndex)*slice;
  }

  private int getBucketIndex(double value) {
    if (value < this.minValue) {
      return 0;
    } else if (value >= this.maxValue) {
      return this.numBuckets + 1;
    } else {
      double gap = value - this.minValue;
      double idx = this.numBuckets * gap / (this.maxValue - this.minValue);
      int i = (int)idx + 1;
      // Check if the index falls in the margin somehow.
      if (value < this.getBucketStartValue(i)) i--;
      else if (value >= this.getBucketEndValue(i)) i++;
      return i; // Due to 1 indexing
    }
  }

  public void refresh() {
    this.refresh(this.numBuckets);
  }

  public void refresh(int numBuckets) {
    this.lock.writeLock().lock();
    try {
      double lowBound = this.minValue;
      double upBound = this.maxValue;
      for (Bucket bucket : this.buckets) {
        if (bucket.count > 0) {
          lowBound = bucket.getMinValue();
          break;
        }
      }
      for (int i = this.buckets.size() - 1; i>=0; i--) {
        Bucket bucket = this.buckets.get(i);
        if (bucket.count > 0) {
          upBound = bucket.getMaxValue();
          break;
        }
      }
      this.refresh(numBuckets, lowBound, upBound);
    } catch (Exception e) {
      LOG.error("Unknown Exception : " + e.getMessage());
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  private void refresh(int numBuckets, double minValue, double maxValue) {
    this.numBuckets = numBuckets;
    this.minValue = minValue;
    this.maxValue = maxValue;

    this.buckets.clear();
    underloadSampleList.clear();
    underload = true;
    buckets.add(new Bucket(Double.MIN_VALUE, this.getBucketStartValue(1)));
    for (int i = 1; i<=this.numBuckets; i++) {
      this.buckets.add(new Bucket(this.getBucketStartValue(i),
          this.getBucketEndValue(i)));
    }
    buckets.add(new Bucket(this.getBucketEndValue(this.numBuckets),
        Double.MAX_VALUE));
  }

  public double getPercentileEstimate(double prcntyl) {
    // We scan from the end of the table since our use case is to find the
    // p99, p95 latencies.
    double originalPrcntyl = prcntyl;
    if (prcntyl > 100.0 || prcntyl < 0.0) {
      throw new IllegalArgumentException("Percentile input value not in range.");
    } else {
      prcntyl = 100.0 - prcntyl;
    }
    double ret = 0.0;
    this.lock.writeLock().lock();
    try {
      if (underloadSampleList.size() == 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Too few data points. Consider increasing the sampling time.");
        }
        return ret;
      } else if (underload) {
        Collections.sort(underloadSampleList);
        // Since the use case is to clear the list after a call to this
        // function, we can afford to sort this list here and enjoy O(1) the
        // rest of the time.
        return underloadSampleList.get(
            (int)(originalPrcntyl * underloadSampleList.size()/100.0));
      }
      int totalCount = 0;
      for (Bucket bucket : this.buckets) {
        totalCount += bucket.count;
      }
      double countToCoverdouble = (totalCount * prcntyl / 100.0);
      int countToCover = (int)countToCoverdouble;
      for (int i=this.buckets.size() - 1; i >= 0; i--) {
        Bucket bucket = this.buckets.get(i);
        if (bucket.getCount() >= countToCover) {
          return bucket.getGreaterValue(bucket.getCount() - countToCover);
        } else {
          countToCover -= bucket.getCount();
        }
      }
      ret = this.maxValue;
    } catch(Exception e) {
      LOG.error("Unknown Exception : " + e.getMessage());
    } finally {
      this.lock.writeLock().unlock();
    }
    return ret;
  }

  public void addValue(double value) {
    this.lock.readLock().lock();
    try {
      if (underloadSampleList.size() >= Histogram.DEFAULT_MINIMUM_LOAD) {
        if (underload) {
          synchronized (underloadSampleList) {
            if (underload) {
              for (double val : underloadSampleList) {
                Bucket bucket = buckets.get(this.getBucketIndex(val));
                bucket.addValue(val);
              }
            }
            underload = false;
          }
        }
        Bucket bucket = buckets.get(this.getBucketIndex(value));
        bucket.addValue(value);
      } else {
        underloadSampleList.add(value);
      }
    } catch (Exception e) {
      LOG.error("Unknown Exception : " + e.getMessage());
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public void addValue(Long value) {
    addValue(value.doubleValue());
  }

  public class Bucket {
    private double sum;
    private int count;
    private double minValue;
    private double maxValue;
    private double startValue;
    private double endValue;
    public Bucket(double startValue, double endValue) {
      this.sum = 0.0;
      this.count = 0;
      this.minValue = endValue;
      this.maxValue = startValue;
      this.startValue = startValue;
      this.endValue = endValue;
    }

    public void addValue(double value) {
      this.sum = this.sum + value;
      count++;
      this.minValue = Math.min(this.minValue, value);
      this.maxValue = Math.max(this.maxValue, value);
    }

    /**
     * This function gives the count of the number of items in the bucket
     * which are smaller than the given value;
     * For the purpose of this calculation, the distribution over the bucket
     * is assumed to be uniformly distributed between minValue and maxValue
     */
    public int getGreaterCount(double value) {
      if (!(this.minValue < value && this.maxValue >= value)) {
        throw new IllegalArgumentException();
      }
      if (this.count == 0) return 0;
      else if (this.count == 1) {
        if (this.minValue > value) return 0;
        else return 1;
      }
      double gap = value - this.minValue;
      double ret = this.count * gap / (this.maxValue - this.minValue);
      return (int)ret;
    }

    /**
     * This function gives the value which is more than a certain count in this
     * bucket.
     * */
    public double getGreaterValue(int count) {
      if (count > this.count) {
        throw new IllegalArgumentException();
      }
      double gap = this.maxValue - this.minValue;
      double ret = this.minValue + gap * count / this.count;
      return ret;
    }

    public double getSum() {
      return this.sum;
    }

    public int getCount() {
      return this.count;
    }

    public double getMinValue() {
      return this.minValue;
    }

    public double getMaxValue() {
      return this.maxValue;
    }

    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      s.append("Bucket Details :");
      s.append(" count : " + this.count);
      s.append(" sum : " + this.sum);
      s.append(" minValue : " + this.minValue);
      s.append(" maxValue : " + this.maxValue);
      s.append(" startValue : " + this.startValue);
      s.append(" endValue : " + this.endValue);
      return s.toString();
    }
  }
}
