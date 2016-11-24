/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY;
import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerResult;
import org.apache.hadoop.hbase.util.RollingStatCalculator;

/**
 * The default implementation for the HeapMemoryTuner. This will do statistical checks on
 * number of evictions, cache misses and flushes to decide whether there should be changes
 * in the heap size of memstore/block cache. During each tuner operation tuner takes a step
 * which can either be INCREASE_BLOCK_CACHE_SIZE (increase block cache size),
 * INCREASE_MEMSTORE_SIZE (increase memstore size) and by default it is NEUTRAL (no change).
 * We say block cache is sufficient when there is no block cache eviction at all or major amount of
 * memory allocated to block cache is empty, similarly we say memory allocated for memstore is
 * sufficient when there is no memstore flushes because of heap pressure or major amount of
 * memory allocated to memstore is empty. If both are sufficient we do nothing, if exactly one of
 * them is found to be sufficient we decrease its size by <i>step</i> and increase the other by
 * same amount. If none of them is sufficient we do statistical analysis on number of cache misses
 * and flushes to determine tuner direction. Based on these statistics we decide the tuner
 * direction. If we are not confident about which step direction to take we do nothing and wait for
 * next iteration. On expectation we will be tuning for at least 10% tuner calls. The number of
 * past periods to consider for statistics calculation can be specified in config by
 * <i>hbase.regionserver.heapmemory.autotuner.lookup.periods</i>. Also these many initial calls to
 * tuner will be ignored (cache is warming up and we leave the system to reach steady state).
 * After the tuner takes a step, in next call we insure that last call was indeed helpful and did
 * not do us any harm. If not then we revert the previous step. The step size is dynamic and it
 * changes based on current and past few tuning directions and their step sizes. We maintain a
 * parameter <i>decayingAvgTunerStepSize</i> which is sum of past tuner steps with
 * sign(positive for increase in memstore and negative for increase in block cache). But rather
 * than simple sum it is calculated by giving more priority to the recent tuning steps.
 * When last few tuner steps were NETURAL then we assume we are restarting the tuning process and
 * step size is updated to maximum allowed size which can be specified  in config by
 * <i>hbase.regionserver.heapmemory.autotuner.step.max</i>. If in a particular tuning operation
 * the step direction is opposite to what indicated by <i>decayingTunerStepSizeSum</i>
 * we decrease the step size by half. Step size does not change in other tuning operations.
 * When step size gets below a certain threshold then the following tuner operations are
 * considered to be neutral. The minimum step size can be specified  in config by
 * <i>hbase.regionserver.heapmemory.autotuner.step.min</i>.
 */
@InterfaceAudience.Private
class DefaultHeapMemoryTuner implements HeapMemoryTuner {
  public static final String MAX_STEP_KEY = "hbase.regionserver.heapmemory.autotuner.step.max";
  public static final String MIN_STEP_KEY = "hbase.regionserver.heapmemory.autotuner.step.min";
  public static final String SUFFICIENT_MEMORY_LEVEL_KEY =
      "hbase.regionserver.heapmemory.autotuner.sufficient.memory.level";
  public static final String LOOKUP_PERIODS_KEY =
      "hbase.regionserver.heapmemory.autotuner.lookup.periods";
  public static final String NUM_PERIODS_TO_IGNORE =
      "hbase.regionserver.heapmemory.autotuner.ignored.periods";
  // Maximum step size that the tuner can take
  public static final float DEFAULT_MAX_STEP_VALUE = 0.04f; // 4%
  // Minimum step size that the tuner can take
  public static final float DEFAULT_MIN_STEP_VALUE = 0.00125f; // 0.125%
  // If current block cache size or memstore size in use is below this level relative to memory
  // provided to it then corresponding component will be considered to have sufficient memory
  public static final float DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE = 0.5f; // 50%
  // Number of tuner periods that will be considered while calculating mean and deviation
  // If set to zero, all stats will be calculated from the start
  public static final int DEFAULT_LOOKUP_PERIODS = 60;
  public static final int DEFAULT_NUM_PERIODS_IGNORED = 60;
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);
  // If deviation of tuner step size gets below this value then it means past few periods were
  // NEUTRAL(given that last tuner period was also NEUTRAL).
  private static final double TUNER_STEP_EPS = 1e-6;

  private Log LOG = LogFactory.getLog(DefaultHeapMemoryTuner.class);
  private TunerResult TUNER_RESULT = new TunerResult(true);
  private Configuration conf;
  private float sufficientMemoryLevel = DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE;
  private float maximumStepSize = DEFAULT_MAX_STEP_VALUE;
  private float minimumStepSize = DEFAULT_MIN_STEP_VALUE;
  private int tunerLookupPeriods = DEFAULT_LOOKUP_PERIODS;
  private int numPeriodsToIgnore = DEFAULT_NUM_PERIODS_IGNORED;
  // Counter to ignore few initial periods while cache is still warming up
  // Memory tuner will do no operation for the first "tunerLookupPeriods"
  private int ignoreInitialPeriods = 0;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  private float globalMemStoreLimitLowMarkPercent;

  // Store statistics about the corresponding parameters for memory tuning
  private RollingStatCalculator rollingStatsForCacheMisses;
  private RollingStatCalculator rollingStatsForFlushes;
  private RollingStatCalculator rollingStatsForEvictions;
  private RollingStatCalculator rollingStatsForTunerSteps;
  // Set step size to max value for tuning, this step size will adjust dynamically while tuning
  private float step = DEFAULT_MAX_STEP_VALUE;
  private StepDirection prevTuneDirection = StepDirection.NEUTRAL;
  //positive means memstore's size was increased
  //It is not just arithmetic sum of past tuner periods. More priority is given to recent
  //tuning steps.
  private double decayingTunerStepSizeSum = 0;

  @Override
  public TunerResult tune(TunerContext context) {
    float curMemstoreSize = context.getCurMemStoreSize();
    float curBlockCacheSize = context.getCurBlockCacheSize();
    addToRollingStats(context);

    if (ignoreInitialPeriods < numPeriodsToIgnore) {
      // Ignoring the first few tuner periods
      ignoreInitialPeriods++;
      rollingStatsForTunerSteps.insertDataValue(0);
      return NO_OP_TUNER_RESULT;
    }
    StepDirection newTuneDirection = getTuneDirection(context);

    float newMemstoreSize;
    float newBlockCacheSize;

    // Adjusting step size for tuning to get to steady state or restart from steady state.
    // Even if the step size was 4% and 32 GB memory size, we will be shifting 1 GB back and forth
    // per tuner operation and it can affect the performance of cluster so we keep on decreasing
    // step size until everything settles.
    if (prevTuneDirection == StepDirection.NEUTRAL
        && newTuneDirection != StepDirection.NEUTRAL
        && rollingStatsForTunerSteps.getDeviation() < TUNER_STEP_EPS) {
      // Restarting the tuning from steady state and setting step size to maximum.
      // The deviation cannot be that low if last period was neutral and some recent periods were
      // not neutral.
      step = maximumStepSize;
    } else if ((newTuneDirection == StepDirection.INCREASE_MEMSTORE_SIZE
        && decayingTunerStepSizeSum < 0) ||
        (newTuneDirection == StepDirection.INCREASE_BLOCK_CACHE_SIZE
        && decayingTunerStepSizeSum > 0)) {
      // Current step is opposite of past tuner actions so decrease the step size to reach steady
      // state.
      step = step/2.00f;
    }
    if (step < minimumStepSize) {
      // If step size is too small then we do nothing.
      LOG.debug("Tuner step size is too low; we will not perform any tuning this time.");
      step = 0.0f;
      newTuneDirection = StepDirection.NEUTRAL;
    }
    // Increase / decrease the memstore / block cahce sizes depending on new tuner step.
    // We don't want to exert immediate pressure on memstore. So, we decrease its size gracefully;
    // we set a minimum bar in the middle of the total memstore size and the lower limit.
    float minMemstoreSize = ((globalMemStoreLimitLowMarkPercent + 1) * curMemstoreSize) / 2.00f;

    switch (newTuneDirection) {
    case INCREASE_BLOCK_CACHE_SIZE:
        if (curMemstoreSize - step < minMemstoreSize) {
          step = curMemstoreSize - minMemstoreSize;
        }
        newMemstoreSize = curMemstoreSize - step;
        newBlockCacheSize = curBlockCacheSize + step;
        rollingStatsForTunerSteps.insertDataValue(-(int)(step*100000));
        decayingTunerStepSizeSum = (decayingTunerStepSizeSum - step)/2.00f;
        break;
    case INCREASE_MEMSTORE_SIZE:
        newBlockCacheSize = curBlockCacheSize - step;
        newMemstoreSize = curMemstoreSize + step;
        rollingStatsForTunerSteps.insertDataValue((int)(step*100000));
        decayingTunerStepSizeSum = (decayingTunerStepSizeSum + step)/2.00f;
        break;
    default:
        prevTuneDirection = StepDirection.NEUTRAL;
        rollingStatsForTunerSteps.insertDataValue(0);
        decayingTunerStepSizeSum = (decayingTunerStepSizeSum)/2.00f;
        return NO_OP_TUNER_RESULT;
    }
    // Check we are within max/min bounds.
    if (newMemstoreSize > globalMemStorePercentMaxRange) {
      newMemstoreSize = globalMemStorePercentMaxRange;
    } else if (newMemstoreSize < globalMemStorePercentMinRange) {
      newMemstoreSize = globalMemStorePercentMinRange;
    }
    if (newBlockCacheSize > blockCachePercentMaxRange) {
      newBlockCacheSize = blockCachePercentMaxRange;
    } else if (newBlockCacheSize < blockCachePercentMinRange) {
      newBlockCacheSize = blockCachePercentMinRange;
    }
    TUNER_RESULT.setBlockCacheSize(newBlockCacheSize);
    TUNER_RESULT.setMemstoreSize(newMemstoreSize);
    prevTuneDirection = newTuneDirection;
    return TUNER_RESULT;
  }

  /**
   * Determine best direction of tuning base on given context.
   * @param context The tuner context.
   * @return tuning direction.
   */
  private StepDirection getTuneDirection(TunerContext context) {
    StepDirection newTuneDirection = StepDirection.NEUTRAL;
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    long cacheMissCount = context.getCacheMissCount();
    long totalFlushCount = blockedFlushCount+unblockedFlushCount;
    float curMemstoreSize = context.getCurMemStoreSize();
    float curBlockCacheSize = context.getCurBlockCacheSize();
    StringBuilder tunerLog = new StringBuilder();
    // We can consider memstore or block cache to be sufficient if
    // we are using only a minor fraction of what have been already provided to it.
    boolean earlyMemstoreSufficientCheck = totalFlushCount == 0
        || context.getCurMemStoreUsed() < curMemstoreSize * sufficientMemoryLevel;
    boolean earlyBlockCacheSufficientCheck = evictCount == 0 ||
        context.getCurBlockCacheUsed() < curBlockCacheSize * sufficientMemoryLevel;
    if (earlyMemstoreSufficientCheck && earlyBlockCacheSufficientCheck) {
      // Both memstore and block cache memory seems to be sufficient. No operation required.
      newTuneDirection = StepDirection.NEUTRAL;
    } else if (earlyMemstoreSufficientCheck) {
      // Increase the block cache size and corresponding decrease in memstore size.
      newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
    } else if (earlyBlockCacheSufficientCheck) {
      // Increase the memstore size and corresponding decrease in block cache size.
      newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
    } else {
      // Early checks for sufficient memory failed. Tuning memory based on past statistics.
      // Boolean indicator to show if we need to revert previous step or not.
      boolean isReverting = false;
      switch (prevTuneDirection) {
      // Here we are using number of evictions rather than cache misses because it is more
      // strong indicator for deficient cache size. Improving caching is what we
      // would like to optimize for in steady state.
      case INCREASE_BLOCK_CACHE_SIZE:
        if ((double)evictCount > rollingStatsForEvictions.getMean() ||
            (double)totalFlushCount > rollingStatsForFlushes.getMean() +
                rollingStatsForFlushes.getDeviation()/2.00) {
          // Reverting previous step as it was not useful.
          // Tuning failed to decrease evictions or tuning resulted in large number of flushes.
          newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          tunerLog.append("We will revert previous tuning");
          if ((double)evictCount > rollingStatsForEvictions.getMean()) {
            tunerLog.append(" because we could not decrease evictions sufficiently.");
          } else {
            tunerLog.append(" because the number of flushes rose significantly.");
          }
          isReverting = true;
        }
        break;
      case INCREASE_MEMSTORE_SIZE:
        if ((double)totalFlushCount > rollingStatsForFlushes.getMean() ||
            (double)evictCount > rollingStatsForEvictions.getMean() +
                rollingStatsForEvictions.getDeviation()/2.00) {
          // Reverting previous step as it was not useful.
          // Tuning failed to decrease flushes or tuning resulted in large number of evictions.
          newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          tunerLog.append("We will revert previous tuning");
          if ((double)totalFlushCount > rollingStatsForFlushes.getMean()) {
            tunerLog.append(" because we could not decrease flushes sufficiently.");
          } else {
            tunerLog.append(" because number of evictions rose significantly.");
          }
          isReverting = true;
        }
        break;
      default:
        // Last step was neutral, revert doesn't not apply here.
        break;
      }
      // If we are not reverting. We try to tune memory sizes by looking at cache misses / flushes.
      if (!isReverting){
        // mean +- deviation*0.8 is considered to be normal
        // below it its consider low and above it is considered high.
        // We can safely assume that the number cache misses, flushes are normally distributed over
        // past periods and hence on all the above mentioned classes (normal, high and low)
        // are likely to occur with probability 56%, 22%, 22% respectively. Hence there is at
        // least ~10% probability that we will not fall in NEUTRAL step.
        // This optimization solution is feedback based and we revert when we
        // dont find our steps helpful. Hence we want to do tuning only when we have clear
        // indications because too many unnecessary tuning may affect the performance of cluster.
        if ((double)cacheMissCount < rollingStatsForCacheMisses.getMean() -
            rollingStatsForCacheMisses.getDeviation()*0.80 &&
            (double)totalFlushCount < rollingStatsForFlushes.getMean() -
                rollingStatsForFlushes.getDeviation()*0.80) {
          // Everything is fine no tuning required
          newTuneDirection = StepDirection.NEUTRAL;
        } else if ((double)cacheMissCount > rollingStatsForCacheMisses.getMean() +
            rollingStatsForCacheMisses.getDeviation()*0.80 &&
            (double)totalFlushCount < rollingStatsForFlushes.getMean() -
                rollingStatsForFlushes.getDeviation()*0.80) {
          // more misses , increasing cache size
          newTuneDirection = StepDirection.INCREASE_BLOCK_CACHE_SIZE;
          tunerLog.append(
              "Going to increase block cache size due to increase in number of cache misses.");
        } else if ((double)cacheMissCount < rollingStatsForCacheMisses.getMean() -
            rollingStatsForCacheMisses.getDeviation()*0.80 &&
            (double)totalFlushCount > rollingStatsForFlushes.getMean() +
                rollingStatsForFlushes.getDeviation()*0.80) {
          // more flushes , increasing memstore size
          newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          tunerLog.append("Going to increase memstore size due to increase in number of flushes.");
        } else if (blockedFlushCount > 0 && prevTuneDirection == StepDirection.NEUTRAL) {
          // we do not want blocked flushes
          newTuneDirection = StepDirection.INCREASE_MEMSTORE_SIZE;
          tunerLog.append("Going to increase memstore size due to"
              + blockedFlushCount + " blocked flushes.");
        } else {
          // Default. Not enough facts to do tuning.
          tunerLog.append("Going to do nothing because we "
              + "could not determine best tuning direction");
          newTuneDirection = StepDirection.NEUTRAL;
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(tunerLog.toString());
    }
    return newTuneDirection;
  }

  /**
   * Add the given context to the rolling tuner stats.
   * @param context The tuner context.
   */
  private void addToRollingStats(TunerContext context) {
    rollingStatsForCacheMisses.insertDataValue(context.getCacheMissCount());
    rollingStatsForFlushes.insertDataValue(context.getBlockedFlushCount() +
        context.getUnblockedFlushCount());
    rollingStatsForEvictions.insertDataValue(context.getEvictCount());
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maximumStepSize = conf.getFloat(MAX_STEP_KEY, DEFAULT_MAX_STEP_VALUE);
    this.minimumStepSize = conf.getFloat(MIN_STEP_KEY, DEFAULT_MIN_STEP_VALUE);
    this.step = this.maximumStepSize;
    this.sufficientMemoryLevel = conf.getFloat(SUFFICIENT_MEMORY_LEVEL_KEY,
        DEFAULT_SUFFICIENT_MEMORY_LEVEL_VALUE);
    this.tunerLookupPeriods = conf.getInt(LOOKUP_PERIODS_KEY, DEFAULT_LOOKUP_PERIODS);
    this.blockCachePercentMinRange = conf.getFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.blockCachePercentMaxRange = conf.getFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.globalMemStorePercentMinRange = conf.getFloat(MEMSTORE_SIZE_MIN_RANGE_KEY,
        MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false));
    this.globalMemStorePercentMaxRange = conf.getFloat(MEMSTORE_SIZE_MAX_RANGE_KEY,
        MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false));
    this.globalMemStoreLimitLowMarkPercent = MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf,
        true);
    // Default value of periods to ignore is number of lookup periods
    this.numPeriodsToIgnore = conf.getInt(NUM_PERIODS_TO_IGNORE, this.tunerLookupPeriods);
    this.rollingStatsForCacheMisses = new RollingStatCalculator(this.tunerLookupPeriods);
    this.rollingStatsForFlushes = new RollingStatCalculator(this.tunerLookupPeriods);
    this.rollingStatsForEvictions = new RollingStatCalculator(this.tunerLookupPeriods);
    this.rollingStatsForTunerSteps = new RollingStatCalculator(this.tunerLookupPeriods);
  }

  private enum StepDirection{
    // block cache size was increased
    INCREASE_BLOCK_CACHE_SIZE,
    // memstore size was increased
    INCREASE_MEMSTORE_SIZE,
    // no operation was performed
    NEUTRAL
  }
}
