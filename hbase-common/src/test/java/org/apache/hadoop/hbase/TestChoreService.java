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
package org.apache.hadoop.hbase;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.CountingChore;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.DoNothingChore;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.FailInitialChore;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.SampleStopper;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.SleepingChore;
import org.apache.hadoop.hbase.TestChoreService.ScheduledChoreSamples.SlowChore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestChoreService {
  /**
   * A few ScheduledChore samples that are useful for testing with ChoreService
   */
  public static class ScheduledChoreSamples {
    /**
     * Straight forward stopper implementation that is used by default when one is not provided
     */
    public static class SampleStopper implements Stoppable {
      private boolean stopped = false;

      @Override
      public void stop(String why) {
        stopped = true;
      }

      @Override
      public boolean isStopped() {
        return stopped;
      }
    }

    /**
     * Sleeps for longer than the scheduled period. This chore always misses its scheduled periodic
     * executions
     */
    public static class SlowChore extends ScheduledChore {
      public SlowChore(String name, int period) {
        this(name, new SampleStopper(), period);
      }

      public SlowChore(String name, Stoppable stopper, int period) {
        super(name, stopper, period);
      }

      @Override
      protected boolean initialChore() {
        try {
          Thread.sleep(getPeriod() * 2);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return true;
      }

      @Override
      protected void chore() {
        try {
          Thread.sleep(getPeriod() * 2);
        } catch (InterruptedException e) {
          //e.printStackTrace();
        }
      }
    }

    /**
     * Lightweight ScheduledChore used primarily to fill the scheduling queue in tests
     */
    public static class DoNothingChore extends ScheduledChore {
      public DoNothingChore(String name, int period) {
        super(name, new SampleStopper(), period);
      }

      public DoNothingChore(String name, Stoppable stopper, int period) {
        super(name, stopper, period);
      }

      @Override
      protected void chore() {
        // DO NOTHING
      }

    }

    public static class SleepingChore extends ScheduledChore {
      private int sleepTime;

      public SleepingChore(String name, int chorePeriod, int sleepTime) {
        this(name, new SampleStopper(), chorePeriod, sleepTime);
      }

      public SleepingChore(String name, Stoppable stopper, int period, int sleepTime) {
        super(name, stopper, period);
        this.sleepTime = sleepTime;
      }

      @Override
      protected boolean initialChore() {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return true;
      }

      @Override
      protected void chore() {
        try {
          Thread.sleep(sleepTime);
        } catch (Exception e) {
          System.err.println(e.getStackTrace());
        }
      }
    }

    public static class CountingChore extends ScheduledChore {
      private int countOfChoreCalls;
      private boolean outputOnTicks = false;

      public CountingChore(String name, int period) {
        this(name, new SampleStopper(), period);
      }

      public CountingChore(String name, Stoppable stopper, int period) {
        this(name, stopper, period, false);
      }

      public CountingChore(String name, Stoppable stopper, int period,
          final boolean outputOnTicks) {
        super(name, stopper, period);
        this.countOfChoreCalls = 0;
        this.outputOnTicks = outputOnTicks;
      }

      @Override
      protected boolean initialChore() {
        countOfChoreCalls++;
        if (outputOnTicks) outputTickCount();
        return true;
      }

      @Override
      protected void chore() {
        countOfChoreCalls++;
        if (outputOnTicks) outputTickCount();
      }

      private void outputTickCount() {
        System.out.println("Chore: " + getName() + ". Count of chore calls: " + countOfChoreCalls);
      }

      public int getCountOfChoreCalls() {
        return countOfChoreCalls;
      }

      public boolean isOutputtingOnTicks() {
        return outputOnTicks;
      }

      public void setOutputOnTicks(boolean o) {
        outputOnTicks = o;
      }
    }

    /**
     * A Chore that will try to execute the initial chore a few times before succeeding. Once the
     * initial chore is complete the chore cancels itself
     */
    public static class FailInitialChore extends ScheduledChore {
      private int numberOfFailures;
      private int failureThreshold;

      /**
       * @param failThreshold Number of times the Chore fails when trying to execute initialChore
       *          before succeeding.
       */
      public FailInitialChore(String name, int period, int failThreshold) {
        this(name, new SampleStopper(), period, failThreshold);
      }

      public FailInitialChore(String name, Stoppable stopper, int period, int failThreshold) {
        super(name, stopper, period);
        numberOfFailures = 0;
        failureThreshold = failThreshold;
      }

      @Override
      protected boolean initialChore() {
        if (numberOfFailures < failureThreshold) {
          numberOfFailures++;
          return false;
        } else {
          return true;
        }
      }

      @Override
      protected void chore() {
        assertTrue(numberOfFailures == failureThreshold);
        cancel(false);
      }

    }
  }

  @Test (timeout=20000)
  public void testInitialChorePrecedence() throws InterruptedException {
    ChoreService service = ChoreService.getInstance("testInitialChorePrecedence");

    final int period = 100;
    final int failureThreshold = 5;

    try {
      ScheduledChore chore = new FailInitialChore("chore", period, failureThreshold);
      service.scheduleChore(chore);

      int loopCount = 0;
      boolean brokeOutOfLoop = false;

     while (!chore.isInitialChoreComplete() && chore.isScheduled()) {
       Thread.sleep(failureThreshold * period);
       loopCount++;
       if (loopCount > 3) {
         brokeOutOfLoop = true;
         break;
       }
    }

    assertFalse(brokeOutOfLoop);
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testCancelChore() throws InterruptedException {
    final int period = 100;
    ScheduledChore chore1 = new DoNothingChore("chore1", period);
    ChoreService service = ChoreService.getInstance("testCancelChore");
    try {
      service.scheduleChore(chore1);
      assertTrue(chore1.isScheduled());

      chore1.cancel(true);
      assertFalse(chore1.isScheduled());
      assertTrue(service.getNumberOfScheduledChores() == 0);
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testScheduledChoreConstruction() {
    final String NAME = "chore";
    final int PERIOD = 100;
    final long VALID_DELAY = 0;
    final long INVALID_DELAY = -100;
    final TimeUnit UNIT = TimeUnit.NANOSECONDS;

    ScheduledChore chore1 =
        new ScheduledChore(NAME, new SampleStopper(), PERIOD, VALID_DELAY, UNIT) {
      @Override
      protected void chore() {
        // DO NOTHING
      }
    };

    assertEquals("Name construction failed", chore1.getName(), NAME);
    assertEquals("Period construction failed", chore1.getPeriod(), PERIOD);
    assertEquals("Initial Delay construction failed", chore1.getInitialDelay(), VALID_DELAY);
    assertEquals("TimeUnit construction failed", chore1.getTimeUnit(), UNIT);

    ScheduledChore invalidDelayChore =
        new ScheduledChore(NAME, new SampleStopper(), PERIOD, INVALID_DELAY, UNIT) {
      @Override
      protected void chore() {
        // DO NOTHING
      }
    };

    assertEquals("Initial Delay should be set to 0 when invalid", 0,
      invalidDelayChore.getInitialDelay());
  }

  @Test (timeout=20000)
  public void testChoreServiceConstruction() throws InterruptedException {
    final int corePoolSize = 10;
    final int defaultCorePoolSize = ChoreService.MIN_CORE_POOL_SIZE;

    ChoreService customInit = new ChoreService("testChoreServiceConstruction_custom", corePoolSize, false);
    try {
      assertEquals(corePoolSize, customInit.getCorePoolSize());
    } finally {
      shutdownService(customInit);
    }

    ChoreService defaultInit = new ChoreService("testChoreServiceConstruction_default");
    try {
      assertEquals(defaultCorePoolSize, defaultInit.getCorePoolSize());
    } finally {
      shutdownService(defaultInit);
    }

    ChoreService invalidInit = new ChoreService("testChoreServiceConstruction_invalid", -10, false);
    try {
      assertEquals(defaultCorePoolSize, invalidInit.getCorePoolSize());
    } finally {
      shutdownService(invalidInit);
    }
  }

  @Test (timeout=20000)
  public void testFrequencyOfChores() throws InterruptedException {
    final int period = 100;
    // Small delta that acts as time buffer (allowing chores to complete if running slowly)
    final int delta = 5;
    ChoreService service = ChoreService.getInstance("testFrequencyOfChores");
    CountingChore chore = new CountingChore("countingChore", period);
    try {
      service.scheduleChore(chore);

      Thread.sleep(10 * period + delta);
      assertTrue(chore.getCountOfChoreCalls() == 11);

      Thread.sleep(10 * period);
      assertTrue(chore.getCountOfChoreCalls() == 21);
    } finally {
      shutdownService(service);
    }
  }

  public void shutdownService(ChoreService service) throws InterruptedException {
    service.shutdown();
    while (!service.isTerminated()) {
      Thread.sleep(100);
    }
  }

  @Test (timeout=20000)
  public void testForceTrigger() throws InterruptedException {
    final int period = 100;
    final int delta = 5;
    ChoreService service = ChoreService.getInstance("testForceTrigger");
    final CountingChore chore = new CountingChore("countingChore", period);
    try {
      service.scheduleChore(chore);
      Thread.sleep(10 * period + delta);

      assertTrue(chore.getCountOfChoreCalls() == 11);

      // Force five runs of the chore to occur, sleeping between triggers to ensure the
      // chore has time to run
      chore.triggerNow();
      Thread.sleep(delta);
      chore.triggerNow();
      Thread.sleep(delta);
      chore.triggerNow();
      Thread.sleep(delta);
      chore.triggerNow();
      Thread.sleep(delta);
      chore.triggerNow();
      Thread.sleep(delta);

      assertTrue("" + chore.getCountOfChoreCalls(), chore.getCountOfChoreCalls() == 16);

      Thread.sleep(10 * period + delta);

      // Be loosey-goosey. It used to be '26' but it was a big flakey relying on timing.
      assertTrue("" + chore.getCountOfChoreCalls(), chore.getCountOfChoreCalls() > 16);
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testCorePoolIncrease() throws InterruptedException {
    final int initialCorePoolSize = 3;
    ChoreService service = new ChoreService("testCorePoolIncrease", initialCorePoolSize, false);

    try {
      assertEquals("Should have a core pool of size: " + initialCorePoolSize, initialCorePoolSize,
        service.getCorePoolSize());

      final int slowChorePeriod = 100;
      SlowChore slowChore1 = new SlowChore("slowChore1", slowChorePeriod);
      SlowChore slowChore2 = new SlowChore("slowChore2", slowChorePeriod);
      SlowChore slowChore3 = new SlowChore("slowChore3", slowChorePeriod);

      service.scheduleChore(slowChore1);
      service.scheduleChore(slowChore2);
      service.scheduleChore(slowChore3);

      Thread.sleep(slowChorePeriod * 10);
      assertEquals("Should not create more pools than scheduled chores", 3,
        service.getCorePoolSize());

      SlowChore slowChore4 = new SlowChore("slowChore4", slowChorePeriod);
      service.scheduleChore(slowChore4);

      Thread.sleep(slowChorePeriod * 10);
      assertEquals("Chores are missing their start time. Should expand core pool size", 4,
        service.getCorePoolSize());

      SlowChore slowChore5 = new SlowChore("slowChore5", slowChorePeriod);
      service.scheduleChore(slowChore5);

      Thread.sleep(slowChorePeriod * 10);
      assertEquals("Chores are missing their start time. Should expand core pool size", 5,
        service.getCorePoolSize());
    } finally {
      shutdownService(service);
    }
  }

  @Test(timeout = 30000)
  public void testCorePoolDecrease() throws InterruptedException {
    final int initialCorePoolSize = 3;
    ChoreService service = new ChoreService("testCorePoolDecrease", initialCorePoolSize, false);
    final int chorePeriod = 100;
    try {
      // Slow chores always miss their start time and thus the core pool size should be at least as
      // large as the number of running slow chores
      SlowChore slowChore1 = new SlowChore("slowChore1", chorePeriod);
      SlowChore slowChore2 = new SlowChore("slowChore2", chorePeriod);
      SlowChore slowChore3 = new SlowChore("slowChore3", chorePeriod);

      service.scheduleChore(slowChore1);
      service.scheduleChore(slowChore2);
      service.scheduleChore(slowChore3);

      Thread.sleep(chorePeriod * 10);
      assertEquals("Should not create more pools than scheduled chores",
        service.getNumberOfScheduledChores(), service.getCorePoolSize());

      SlowChore slowChore4 = new SlowChore("slowChore4", chorePeriod);
      service.scheduleChore(slowChore4);
      Thread.sleep(chorePeriod * 10);
      assertEquals("Chores are missing their start time. Should expand core pool size",
        service.getNumberOfScheduledChores(), service.getCorePoolSize());

      SlowChore slowChore5 = new SlowChore("slowChore5", chorePeriod);
      service.scheduleChore(slowChore5);
      Thread.sleep(chorePeriod * 10);
      assertEquals("Chores are missing their start time. Should expand core pool size",
        service.getNumberOfScheduledChores(), service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 5);

      // Now we begin to cancel the chores that caused an increase in the core thread pool of the
      // ChoreService. These cancellations should cause a decrease in the core thread pool.
      slowChore5.cancel();
      Thread.sleep(chorePeriod * 10);
      assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
        service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 4);

      slowChore4.cancel();
      Thread.sleep(chorePeriod * 10);
      assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
        service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 3);

      slowChore3.cancel();
      Thread.sleep(chorePeriod * 10);
      assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
        service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 2);

      slowChore2.cancel();
      Thread.sleep(chorePeriod * 10);
      assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
        service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 1);

      slowChore1.cancel();
      Thread.sleep(chorePeriod * 10);
      assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
        service.getCorePoolSize());
      assertEquals(service.getNumberOfChoresMissingStartTime(), 0);
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testNumberOfRunningChores() throws InterruptedException {
    ChoreService service = new ChoreService("testNumberOfRunningChores");

    final int period = 100;
    final int sleepTime = 5;

    try {
      DoNothingChore dn1 = new DoNothingChore("dn1", period);
      DoNothingChore dn2 = new DoNothingChore("dn2", period);
      DoNothingChore dn3 = new DoNothingChore("dn3", period);
      DoNothingChore dn4 = new DoNothingChore("dn4", period);
      DoNothingChore dn5 = new DoNothingChore("dn5", period);

      service.scheduleChore(dn1);
      service.scheduleChore(dn2);
      service.scheduleChore(dn3);
      service.scheduleChore(dn4);
      service.scheduleChore(dn5);

      Thread.sleep(sleepTime);
      assertEquals("Scheduled chore mismatch", 5, service.getNumberOfScheduledChores());

      dn1.cancel();
      Thread.sleep(sleepTime);
      assertEquals("Scheduled chore mismatch", 4, service.getNumberOfScheduledChores());

      dn2.cancel();
      dn3.cancel();
      dn4.cancel();
      Thread.sleep(sleepTime);
      assertEquals("Scheduled chore mismatch", 1, service.getNumberOfScheduledChores());

      dn5.cancel();
      Thread.sleep(sleepTime);
      assertEquals("Scheduled chore mismatch", 0, service.getNumberOfScheduledChores());
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testNumberOfChoresMissingStartTime() throws InterruptedException {
    ChoreService service = new ChoreService("testNumberOfChoresMissingStartTime");

    final int period = 100;
    final int sleepTime = 5 * period;

    try {
      // Slow chores sleep for a length of time LONGER than their period. Thus, SlowChores
      // ALWAYS miss their start time since their execution takes longer than their period
      SlowChore sc1 = new SlowChore("sc1", period);
      SlowChore sc2 = new SlowChore("sc2", period);
      SlowChore sc3 = new SlowChore("sc3", period);
      SlowChore sc4 = new SlowChore("sc4", period);
      SlowChore sc5 = new SlowChore("sc5", period);

      service.scheduleChore(sc1);
      service.scheduleChore(sc2);
      service.scheduleChore(sc3);
      service.scheduleChore(sc4);
      service.scheduleChore(sc5);

      Thread.sleep(sleepTime);
      assertEquals(5, service.getNumberOfChoresMissingStartTime());

      sc1.cancel();
      Thread.sleep(sleepTime);
      assertEquals(4, service.getNumberOfChoresMissingStartTime());

      sc2.cancel();
      sc3.cancel();
      sc4.cancel();
      Thread.sleep(sleepTime);
      assertEquals(1, service.getNumberOfChoresMissingStartTime());

      sc5.cancel();
      Thread.sleep(sleepTime);
      assertEquals(0, service.getNumberOfChoresMissingStartTime());
    } finally {
      shutdownService(service);
    }
  }

  /**
   * ChoreServices should never have a core pool size that exceeds the number of chores that have
   * been scheduled with the service. For example, if 4 ScheduledChores are scheduled with a
   * ChoreService, the number of threads in the ChoreService's core pool should never exceed 4
   */
  @Test (timeout=20000)
  public void testMaximumChoreServiceThreads() throws InterruptedException {
    ChoreService service = new ChoreService("testMaximumChoreServiceThreads");

    final int period = 100;
    final int sleepTime = 5 * period;

    try {
      // Slow chores sleep for a length of time LONGER than their period. Thus, SlowChores
      // ALWAYS miss their start time since their execution takes longer than their period.
      // Chores that miss their start time will trigger the onChoreMissedStartTime callback
      // in the ChoreService. This callback will try to increase the number of core pool
      // threads.
      SlowChore sc1 = new SlowChore("sc1", period);
      SlowChore sc2 = new SlowChore("sc2", period);
      SlowChore sc3 = new SlowChore("sc3", period);
      SlowChore sc4 = new SlowChore("sc4", period);
      SlowChore sc5 = new SlowChore("sc5", period);

      service.scheduleChore(sc1);
      service.scheduleChore(sc2);
      service.scheduleChore(sc3);
      service.scheduleChore(sc4);
      service.scheduleChore(sc5);

      Thread.sleep(sleepTime);
      assertTrue(service.getCorePoolSize() <= service.getNumberOfScheduledChores());

      SlowChore sc6 = new SlowChore("sc6", period);
      SlowChore sc7 = new SlowChore("sc7", period);
      SlowChore sc8 = new SlowChore("sc8", period);
      SlowChore sc9 = new SlowChore("sc9", period);
      SlowChore sc10 = new SlowChore("sc10", period);

      service.scheduleChore(sc6);
      service.scheduleChore(sc7);
      service.scheduleChore(sc8);
      service.scheduleChore(sc9);
      service.scheduleChore(sc10);

      Thread.sleep(sleepTime);
      assertTrue(service.getCorePoolSize() <= service.getNumberOfScheduledChores());
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testChangingChoreServices() throws InterruptedException {
    final int period = 100;
    final int sleepTime = 10;
    ChoreService service1 = new ChoreService("testChangingChoreServices_1");
    ChoreService service2 = new ChoreService("testChangingChoreServices_2");
    ScheduledChore chore = new DoNothingChore("sample", period);

    try {
      assertFalse(chore.isScheduled());
      assertFalse(service1.isChoreScheduled(chore));
      assertFalse(service2.isChoreScheduled(chore));
      assertTrue(chore.getChoreServicer() == null);

      service1.scheduleChore(chore);
      Thread.sleep(sleepTime);
      assertTrue(chore.isScheduled());
      assertTrue(service1.isChoreScheduled(chore));
      assertFalse(service2.isChoreScheduled(chore));
      assertFalse(chore.getChoreServicer() == null);

      service2.scheduleChore(chore);
      Thread.sleep(sleepTime);
      assertTrue(chore.isScheduled());
      assertFalse(service1.isChoreScheduled(chore));
      assertTrue(service2.isChoreScheduled(chore));
      assertFalse(chore.getChoreServicer() == null);

      chore.cancel();
      assertFalse(chore.isScheduled());
      assertFalse(service1.isChoreScheduled(chore));
      assertFalse(service2.isChoreScheduled(chore));
      assertTrue(chore.getChoreServicer() == null);
    } finally {
      shutdownService(service1);
      shutdownService(service2);
    }
  }

  @Test (timeout=20000)
  public void testTriggerNowFailsWhenNotScheduled() throws InterruptedException {
    final int period = 100;
    // Small sleep time buffer to allow CountingChore to complete
    final int sleep = 5;
    ChoreService service = new ChoreService("testTriggerNowFailsWhenNotScheduled");
    CountingChore chore = new CountingChore("dn", period);

    try {
      assertFalse(chore.triggerNow());
      assertTrue(chore.getCountOfChoreCalls() == 0);

      service.scheduleChore(chore);
      Thread.sleep(sleep);
      assertEquals(1, chore.getCountOfChoreCalls());
      Thread.sleep(period);
      assertEquals(2, chore.getCountOfChoreCalls());
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertTrue(chore.triggerNow());
      Thread.sleep(sleep);
      assertEquals(5, chore.getCountOfChoreCalls());
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testStopperForScheduledChores() throws InterruptedException {
    ChoreService service = ChoreService.getInstance("testStopperForScheduledChores");
    Stoppable stopperForGroup1 = new SampleStopper();
    Stoppable stopperForGroup2 = new SampleStopper();
    final int period = 100;
    final int delta = 10;

    try {
      ScheduledChore chore1_group1 = new DoNothingChore("c1g1", stopperForGroup1, period);
      ScheduledChore chore2_group1 = new DoNothingChore("c2g1", stopperForGroup1, period);
      ScheduledChore chore3_group1 = new DoNothingChore("c3g1", stopperForGroup1, period);

      ScheduledChore chore1_group2 = new DoNothingChore("c1g2", stopperForGroup2, period);
      ScheduledChore chore2_group2 = new DoNothingChore("c2g2", stopperForGroup2, period);
      ScheduledChore chore3_group2 = new DoNothingChore("c3g2", stopperForGroup2, period);

      service.scheduleChore(chore1_group1);
      service.scheduleChore(chore2_group1);
      service.scheduleChore(chore3_group1);
      service.scheduleChore(chore1_group2);
      service.scheduleChore(chore2_group2);
      service.scheduleChore(chore3_group2);

      Thread.sleep(delta);
      Thread.sleep(10 * period);
      assertTrue(chore1_group1.isScheduled());
      assertTrue(chore2_group1.isScheduled());
      assertTrue(chore3_group1.isScheduled());
      assertTrue(chore1_group2.isScheduled());
      assertTrue(chore2_group2.isScheduled());
      assertTrue(chore3_group2.isScheduled());

      stopperForGroup1.stop("test stopping group 1");
      Thread.sleep(period);
      assertFalse(chore1_group1.isScheduled());
      assertFalse(chore2_group1.isScheduled());
      assertFalse(chore3_group1.isScheduled());
      assertTrue(chore1_group2.isScheduled());
      assertTrue(chore2_group2.isScheduled());
      assertTrue(chore3_group2.isScheduled());

      stopperForGroup2.stop("test stopping group 2");
      Thread.sleep(period);
      assertFalse(chore1_group1.isScheduled());
      assertFalse(chore2_group1.isScheduled());
      assertFalse(chore3_group1.isScheduled());
      assertFalse(chore1_group2.isScheduled());
      assertFalse(chore2_group2.isScheduled());
      assertFalse(chore3_group2.isScheduled());
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testShutdownCancelsScheduledChores() throws InterruptedException {
    final int period = 100;
    ChoreService service = new ChoreService("testShutdownCancelsScheduledChores");
    ScheduledChore successChore1 = new DoNothingChore("sc1", period);
    ScheduledChore successChore2 = new DoNothingChore("sc2", period);
    ScheduledChore successChore3 = new DoNothingChore("sc3", period);

    try {
      assertTrue(service.scheduleChore(successChore1));
      assertTrue(successChore1.isScheduled());
      assertTrue(service.scheduleChore(successChore2));
      assertTrue(successChore2.isScheduled());
      assertTrue(service.scheduleChore(successChore3));
      assertTrue(successChore3.isScheduled());
    } finally {
      shutdownService(service);
    }

    assertFalse(successChore1.isScheduled());
    assertFalse(successChore2.isScheduled());
    assertFalse(successChore3.isScheduled());
  }

  @Test (timeout=20000)
  public void testShutdownWorksWhileChoresAreExecuting() throws InterruptedException {
    final int period = 100;
    final int sleep = 5 * period;
    ChoreService service = new ChoreService("testShutdownWorksWhileChoresAreExecuting");
    ScheduledChore slowChore1 = new SleepingChore("sc1", period, sleep);
    ScheduledChore slowChore2 = new SleepingChore("sc2", period, sleep);
    ScheduledChore slowChore3 = new SleepingChore("sc3", period, sleep);
    try {
      assertTrue(service.scheduleChore(slowChore1));
      assertTrue(service.scheduleChore(slowChore2));
      assertTrue(service.scheduleChore(slowChore3));

      Thread.sleep(sleep / 2);
      shutdownService(service);

      assertFalse(slowChore1.isScheduled());
      assertFalse(slowChore2.isScheduled());
      assertFalse(slowChore3.isScheduled());
      assertTrue(service.isShutdown());

      Thread.sleep(5);
      assertTrue(service.isTerminated());
    } finally {
      shutdownService(service);
    }
  }

  @Test (timeout=20000)
  public void testShutdownRejectsNewSchedules() throws InterruptedException {
    final int period = 100;
    ChoreService service = new ChoreService("testShutdownRejectsNewSchedules");
    ScheduledChore successChore1 = new DoNothingChore("sc1", period);
    ScheduledChore successChore2 = new DoNothingChore("sc2", period);
    ScheduledChore successChore3 = new DoNothingChore("sc3", period);
    ScheduledChore failChore1 = new DoNothingChore("fc1", period);
    ScheduledChore failChore2 = new DoNothingChore("fc2", period);
    ScheduledChore failChore3 = new DoNothingChore("fc3", period);

    try {
      assertTrue(service.scheduleChore(successChore1));
      assertTrue(successChore1.isScheduled());
      assertTrue(service.scheduleChore(successChore2));
      assertTrue(successChore2.isScheduled());
      assertTrue(service.scheduleChore(successChore3));
      assertTrue(successChore3.isScheduled());
    } finally {
      shutdownService(service);
    }

    assertFalse(service.scheduleChore(failChore1));
    assertFalse(failChore1.isScheduled());
    assertFalse(service.scheduleChore(failChore2));
    assertFalse(failChore2.isScheduled());
    assertFalse(service.scheduleChore(failChore3));
    assertFalse(failChore3.isScheduled());
  }
}
