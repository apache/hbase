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

package org.apache.hadoop.hbase;

import java.text.MessageFormat;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * A class that provides a standard waitFor pattern
 * See details at https://issues.apache.org/jira/browse/HBASE-7384
 */
@InterfaceAudience.Private
public final class Waiter {

  private static final Log LOG = LogFactory.getLog(Waiter.class);

  /**
   * System property name whose value is a scale factor to increase time out values dynamically used
   * in {@link #sleep(Configuration, long)}, {@link #waitFor(Configuration, long, Predicate)},
   * {@link #waitFor(Configuration, long, long, Predicate)}, and
   * {@link #waitFor(Configuration, long, long, boolean, Predicate)} method
   * <p/>
   * The actual time out value will equal to hbase.test.wait.for.ratio * passed-in timeout
   */
  public static final String HBASE_TEST_WAIT_FOR_RATIO = "hbase.test.wait.for.ratio";

  private static float HBASE_WAIT_FOR_RATIO_DEFAULT = 1;

  private static float waitForRatio = -1;

  private Waiter() {
  }

  /**
   * Returns the 'wait for ratio' used in the {@link #sleep(Configuration, long)},
   * {@link #waitFor(Configuration, long, Predicate)},
   * {@link #waitFor(Configuration, long, long, Predicate)} and
   * {@link #waitFor(Configuration, long, long, boolean, Predicate)} methods of the class
   * <p/>
   * This is useful to dynamically adjust max time out values when same test cases run in different
   * test machine settings without recompiling & re-deploying code.
   * <p/>
   * The value is obtained from the Java System property or configuration setting
   * <code>hbase.test.wait.for.ratio</code> which defaults to <code>1</code>.
   * @param conf the configuration
   * @return the 'wait for ratio' for the current test run.
   */
  public static float getWaitForRatio(Configuration conf) {
    if (waitForRatio < 0) {
      // System property takes precedence over configuration setting
      if (System.getProperty(HBASE_TEST_WAIT_FOR_RATIO) != null) {
        waitForRatio = Float.parseFloat(System.getProperty(HBASE_TEST_WAIT_FOR_RATIO));
      } else {
        waitForRatio = conf.getFloat(HBASE_TEST_WAIT_FOR_RATIO, HBASE_WAIT_FOR_RATIO_DEFAULT);
      }
    }
    return waitForRatio;
  }

  /**
   * A predicate 'closure' used by the {@link Waiter#waitFor(Configuration, long, Predicate)} and
   * {@link Waiter#waitFor(Configuration, long, Predicate)} and
   * {@link Waiter#waitFor(Configuration, long, long, boolean, Predicate) methods.
   */
  @InterfaceAudience.Private
  public interface Predicate<E extends Exception> {

    /**
     * Perform a predicate evaluation.
     * @return the boolean result of the evaluation.
     * @throws Exception thrown if the predicate evaluation could not evaluate.
     */
    boolean evaluate() throws E;

  }

  /**
   * Makes the current thread sleep for the duration equal to the specified time in milliseconds
   * multiplied by the {@link #getWaitForRatio(Configuration)}.
   * @param conf the configuration
   * @param time the number of milliseconds to sleep.
   */
  public static void sleep(Configuration conf, long time) {
    try {
      Thread.sleep((long) (getWaitForRatio(conf) * time));
    } catch (InterruptedException ex) {
      LOG.warn(MessageFormat.format("Sleep interrupted, {0}", ex.toString()));
    }
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached and the Predicate is still
   * <code>false</code>.
   * <p/>
   * @param conf the configuration
   * @param timeout the timeout in milliseconds to wait for the predicate.
   * @param predicate the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout,
      Predicate<E> predicate) throws E {
    return waitFor(conf, timeout, 100, true, predicate);
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached and the Predicate is still
   * <code>false</code>.
   * <p/>
   * @param conf the configuration
   * @param timeout the max timeout in milliseconds to wait for the predicate.
   * @param interval the interval in milliseconds to evaluate predicate.
   * @param predicate the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout, long interval,
      Predicate<E> predicate) throws E {
    return waitFor(conf, timeout, interval, true, predicate);
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached, the Predicate is still
   * <code>false</code> and failIfTimeout is set as <code>true</code>.
   * <p/>
   * @param conf the configuration
   * @param timeout the timeout in milliseconds to wait for the predicate.
   * @param interval the interval in milliseconds to evaluate predicate.
   * @param failIfTimeout indicates if should fail current test case when times out.
   * @param predicate the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout, long interval,
      boolean failIfTimeout, Predicate<E> predicate) throws E {
    long started = System.currentTimeMillis();
    long adjustedTimeout = (long) (getWaitForRatio(conf) * timeout);
    long mustEnd = started + adjustedTimeout;
    long remainderWait = 0;
    long sleepInterval = 0;
    Boolean eval = false;
    Boolean interrupted = false;

    try {
      LOG.info(MessageFormat.format("Waiting up to [{0}] milli-secs(wait.for.ratio=[{1}])",
        adjustedTimeout, getWaitForRatio(conf)));
      while (!(eval = predicate.evaluate())
              && (remainderWait = mustEnd - System.currentTimeMillis()) > 0) {
        try {
          // handle tail case when remainder wait is less than one interval
          sleepInterval = (remainderWait > interval) ? interval : remainderWait;
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          eval = predicate.evaluate();
          interrupted = true;
          break;
        }
      }
      if (!eval) {
        if (interrupted) {
          LOG.warn(MessageFormat.format("Waiting interrupted after [{0}] msec",
            System.currentTimeMillis() - started));
        } else if (failIfTimeout) {
          Assert.fail(MessageFormat.format("Waiting timed out after [{0}] msec", adjustedTimeout));
        } else {
          LOG.warn(MessageFormat.format("Waiting timed out after [{0}] msec", adjustedTimeout));
        }
      }
      return (eval || interrupted) ? (System.currentTimeMillis() - started) : -1;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
