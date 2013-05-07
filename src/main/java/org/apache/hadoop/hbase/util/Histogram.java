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

import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.Log;

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
  private Double minValue;
  private Double maxValue;
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  public static final Log LOG = LogFactory.getLog(Histogram.class.getName());

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
  public Histogram(int numBuckets, Double minValue, Double maxValue) {
    if (numBuckets < 1 || minValue >= maxValue) {
      throw new UnsupportedOperationException();
    }
    buckets = new ArrayList<Bucket>(numBuckets);
    refresh(numBuckets, minValue, maxValue);
  }

  // This is included in the bucket
  private Double getBucketStartValue(int bucketIndex) {
    if (bucketIndex < 1 || bucketIndex > this.numBuckets) {
      throw new IndexOutOfBoundsException();
    }
    Double gap = this.maxValue - this.minValue;
    Double slice = gap/this.numBuckets;
    return this.minValue + (bucketIndex - 1.0)*slice;
  }

  //This is not included in the bucket
  private Double getBucketEndValue(int bucketIndex) {
    if (bucketIndex < 1 || bucketIndex > this.numBuckets) {
      throw new IndexOutOfBoundsException();
    }
    Double gap = this.maxValue - this.minValue;
    Double slice = gap/this.numBuckets;
    return this.minValue + (bucketIndex)*slice;
  }

  private int getBucketIndex(Double value) {
    if (value < this.minValue) {
      return 0;
    } else if (value >= this.maxValue) {
      return this.numBuckets + 1;
    } else {
      Double gap = value - this.minValue;
      Double idx = this.numBuckets * gap / (this.maxValue - this.minValue);
      int i = idx.intValue() + 1;
      // Check if the index falls in the margin somehow.
      if (value < this.getBucketStartValue(i)) i--;
      else if (value >= this.getBucketEndValue(i)) i++;
      return i; // Due to 1 indexing
    }
  }

  public void refresh(int numBuckets) {
    this.lock.writeLock().lock();
    try {
      Double minValue = this.minValue;
      Double maxValue = this.maxValue;
      for (Bucket bucket : this.buckets) {
        if (bucket.count > 0) {
          minValue = bucket.getMinValue();
          break;
        }
      }
      for (int i = this.buckets.size() - 1; i>=0; i--) {
        Bucket bucket = this.buckets.get(i);
        if (bucket.count > 0) {
          maxValue = bucket.getMaxValue();
          break;
        }
      }
      this.refresh(numBuckets, minValue, maxValue);
    } catch (Exception e) {
      LOG.error("Unknown Exception : " + e.getMessage());
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  private void refresh(int numBuckets, Double minValue, Double maxValue) {
    this.numBuckets = numBuckets;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.buckets.clear();
    buckets.add(new Bucket(Double.MIN_VALUE, this.getBucketStartValue(1)));
    for (int i = 1; i<=this.numBuckets; i++) {
      this.buckets.add(new Bucket(this.getBucketStartValue(i),
          this.getBucketEndValue(i)));
    }
    buckets.add(new Bucket(this.getBucketEndValue(this.numBuckets),
        Double.MAX_VALUE));
  }

  public Double getPercentileEstimate(Double prcntyl) {
    // We scan from the end of the table since our use case is to find the
    // p99, p95 latencies.
    if (prcntyl > 100.0 || prcntyl < 0.0) {
      throw new IllegalArgumentException("Percentile input value not in range.");
    } else {
      prcntyl = 100.0 - prcntyl;
    }
    Double ret = 0.0;
    this.lock.writeLock().lock();
    try {
      int totalCount = 0;
      for (Bucket bucket : this.buckets) {
        totalCount += bucket.count;
      }
      if (totalCount == 0) {
        throw new UnsupportedOperationException("Too few data points.");
      }
      Double countToCoverDouble = (totalCount * prcntyl / 100.0);
      int countToCover = countToCoverDouble.intValue();
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

  public void addValue(Double value) {
    this.lock.readLock().lock();
    try {
      Bucket bucket = buckets.get(this.getBucketIndex(value));
      bucket.addValue(value);
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
    private Double sum;
    private int count;
    private Double minValue;
    private Double maxValue;
    private Double startValue;
    private Double endValue;
    public Bucket(Double startValue, Double endValue) {
      this.sum = 0.0;
      this.count = 0;
      this.minValue = endValue;
      this.maxValue = startValue;
      this.startValue = startValue;
      this.endValue = endValue;
    }

    public void addValue(Double value) {
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
    public int getGreaterCount(Double value) {
      if (!(this.minValue < value && this.maxValue >= value)) {
        throw new IllegalArgumentException();
      }
      if (this.count == 0) return 0;
      else if (this.count == 1) {
        if (this.minValue > value) return 0;
        else return 1;
      }
      Double gap = value - this.minValue;
      Double ret = this.count * gap / (this.maxValue - this.minValue);
      return ret.intValue();
    }

    /**
     * This function gives the value which is more than a certain count in this
     * bucket.
     * */
    public Double getGreaterValue(int count) {
      if (count > this.count) {
        throw new IllegalArgumentException();
      }
      if (count == 0) return this.endValue;
      Double gap = this.maxValue - this.minValue;
      Double ret = this.minValue + gap * count / this.count;
      return ret;
    }

    public Double getSum() {
      return this.sum;
    }

    public int getCount() {
      return this.count;
    }

    public Double getMinValue() {
      return this.minValue;
    }

    public Double getMaxValue() {
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
