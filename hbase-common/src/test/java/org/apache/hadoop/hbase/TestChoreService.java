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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
public class TestChoreService {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestChoreService.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestChoreService.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  @Rule
  public TestName name = new TestName();

  private int initialCorePoolSize = 3;

  private ChoreService service;

  @Before
  public void setUp() {
    service = new ChoreService(name.getMethodName(), initialCorePoolSize, false);
  }

  @After
  public void tearDown() {
    shutdownService(service);
  }

  /**
   * Straight forward stopper implementation that is used by default when one is not provided
   */
  private static class SampleStopper implements Stoppable {
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
  private static class SlowChore extends ScheduledChore {
    public SlowChore(String name, int period) {
      this(name, new SampleStopper(), period);
    }

    public SlowChore(String name, Stoppable stopper, int period) {
      super(name, stopper, period);
    }

    @Override
    protected boolean initialChore() {
      Threads.sleep(getPeriod() * 2);
      return true;
    }

    @Override
    protected void chore() {
      Threads.sleep(getPeriod() * 2);
    }
  }

  /**
   * Lightweight ScheduledChore used primarily to fill the scheduling queue in tests
   */
  private static class DoNothingChore extends ScheduledChore {

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

  private static class SleepingChore extends ScheduledChore {
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
      Threads.sleep(sleepTime);
      return true;
    }

    @Override
    protected void chore() {
      Threads.sleep(sleepTime);
    }
  }

  private static class CountingChore extends ScheduledChore {
    private int countOfChoreCalls;
    private boolean outputOnTicks = false;

    public CountingChore(String name, int period) {
      this(name, new SampleStopper(), period);
    }

    public CountingChore(String name, Stoppable stopper, int period) {
      this(name, stopper, period, false);
    }

    public CountingChore(String name, Stoppable stopper, int period, final boolean outputOnTicks) {
      super(name, stopper, period);
      this.countOfChoreCalls = 0;
      this.outputOnTicks = outputOnTicks;
    }

    @Override
    protected boolean initialChore() {
      countOfChoreCalls++;
      if (outputOnTicks) {
        outputTickCount();
      }
      return true;
    }

    @Override
    protected void chore() {
      countOfChoreCalls++;
      if (outputOnTicks) {
        outputTickCount();
      }
    }

    private void outputTickCount() {
      LOG.info("Chore: " + getName() + ". Count of chore calls: " + countOfChoreCalls);
    }

    public int getCountOfChoreCalls() {
      return countOfChoreCalls;
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

  @Test
  public void testInitialChorePrecedence() throws InterruptedException {
    final int period = 100;
    final int failureThreshold = 5;
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
  }

  @Test
  public void testCancelChore() throws InterruptedException {
    final int period = 100;
    ScheduledChore chore = new DoNothingChore("chore", period);
    service.scheduleChore(chore);
    assertTrue(chore.isScheduled());

    chore.cancel(true);
    assertFalse(chore.isScheduled());
    assertTrue(service.getNumberOfScheduledChores() == 0);
  }

  @Test
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

    assertEquals("Name construction failed", NAME, chore1.getName());
    assertEquals("Period construction failed", PERIOD, chore1.getPeriod());
    assertEquals("Initial Delay construction failed", VALID_DELAY, chore1.getInitialDelay());
    assertEquals("TimeUnit construction failed", UNIT, chore1.getTimeUnit());

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

  @Test
  public void testChoreServiceConstruction() throws InterruptedException {
    final int corePoolSize = 10;
    final int defaultCorePoolSize = ChoreService.MIN_CORE_POOL_SIZE;

    ChoreService customInit =
      new ChoreService("testChoreServiceConstruction_custom", corePoolSize, false);
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

  @Test
  public void testFrequencyOfChores() throws InterruptedException {
    final int period = 100;
    // Small delta that acts as time buffer (allowing chores to complete if running slowly)
    final int delta = period / 5;
    CountingChore chore = new CountingChore("countingChore", period);
    service.scheduleChore(chore);

    Thread.sleep(10 * period + delta);
    assertEquals("10 periods have elapsed.", 11, chore.getCountOfChoreCalls());

    Thread.sleep(10 * period + delta);
    assertEquals("20 periods have elapsed.", 21, chore.getCountOfChoreCalls());
  }

  public void shutdownService(ChoreService service) {
    service.shutdown();
    Waiter.waitFor(CONF, 1000, () -> service.isTerminated());
  }

  @Test
  public void testForceTrigger() throws InterruptedException {
    final int period = 100;
    final int delta = period / 10;
    final CountingChore chore = new CountingChore("countingChore", period);
    service.scheduleChore(chore);
    Thread.sleep(10 * period + delta);

    assertEquals("10 periods have elapsed.", 11, chore.getCountOfChoreCalls());

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

    assertEquals("Trigger was called 5 times after 10 periods.", 16, chore.getCountOfChoreCalls());

    Thread.sleep(10 * period + delta);

    // Be loosey-goosey. It used to be '26' but it was a big flakey relying on timing.
    assertTrue("Expected at least 16 invocations, instead got " + chore.getCountOfChoreCalls(),
      chore.getCountOfChoreCalls() > 16);
  }

  @Test
  public void testCorePoolIncrease() throws InterruptedException {
    assertEquals("Setting core pool size gave unexpected results.", initialCorePoolSize,
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
  }

  @Test
  public void testCorePoolDecrease() throws InterruptedException {
    final int chorePeriod = 100;
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
    assertEquals(5, service.getNumberOfChoresMissingStartTime());

    // Now we begin to cancel the chores that caused an increase in the core thread pool of the
    // ChoreService. These cancellations should cause a decrease in the core thread pool.
    slowChore5.cancel();
    Thread.sleep(chorePeriod * 10);
    assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
      service.getCorePoolSize());
    assertEquals(4, service.getNumberOfChoresMissingStartTime());

    slowChore4.cancel();
    Thread.sleep(chorePeriod * 10);
    assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
      service.getCorePoolSize());
    assertEquals(3, service.getNumberOfChoresMissingStartTime());

    slowChore3.cancel();
    Thread.sleep(chorePeriod * 10);
    assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
      service.getCorePoolSize());
    assertEquals(2, service.getNumberOfChoresMissingStartTime());

    slowChore2.cancel();
    Thread.sleep(chorePeriod * 10);
    assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
      service.getCorePoolSize());
    assertEquals(1, service.getNumberOfChoresMissingStartTime());

    slowChore1.cancel();
    Thread.sleep(chorePeriod * 10);
    assertEquals(Math.max(ChoreService.MIN_CORE_POOL_SIZE, service.getNumberOfScheduledChores()),
      service.getCorePoolSize());
    assertEquals(0, service.getNumberOfChoresMissingStartTime());
  }

  @Test
  public void testNumberOfRunningChores() throws InterruptedException {
    final int period = 100;
    final int sleepTime = 5;
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
  }

  @Test
  public void testNumberOfChoresMissingStartTime() throws InterruptedException {
    final int period = 100;
    final int sleepTime = 20 * period;
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
  }

  /**
   * ChoreServices should never have a core pool size that exceeds the number of chores that have
   * been scheduled with the service. For example, if 4 ScheduledChores are scheduled with a
   * ChoreService, the number of threads in the ChoreService's core pool should never exceed 4
   */
  @Test
  public void testMaximumChoreServiceThreads() throws InterruptedException {

    final int period = 100;
    final int sleepTime = 5 * period;
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
  }

  @Test
  public void testChangingChoreServices() throws InterruptedException {
    final int period = 100;
    final int sleepTime = 10;
    ChoreService anotherService = new ChoreService(name.getMethodName() + "_2");
    ScheduledChore chore = new DoNothingChore("sample", period);

    try {
      assertFalse(chore.isScheduled());
      assertFalse(service.isChoreScheduled(chore));
      assertFalse(anotherService.isChoreScheduled(chore));
      assertTrue(chore.getChoreService() == null);

      service.scheduleChore(chore);
      Thread.sleep(sleepTime);
      assertTrue(chore.isScheduled());
      assertTrue(service.isChoreScheduled(chore));
      assertFalse(anotherService.isChoreScheduled(chore));
      assertFalse(chore.getChoreService() == null);

      anotherService.scheduleChore(chore);
      Thread.sleep(sleepTime);
      assertTrue(chore.isScheduled());
      assertFalse(service.isChoreScheduled(chore));
      assertTrue(anotherService.isChoreScheduled(chore));
      assertFalse(chore.getChoreService() == null);

      chore.cancel();
      assertFalse(chore.isScheduled());
      assertFalse(service.isChoreScheduled(chore));
      assertFalse(anotherService.isChoreScheduled(chore));
      assertTrue(chore.getChoreService() == null);
    } finally {
      shutdownService(anotherService);
    }
  }

  @Test
  public void testStopperForScheduledChores() throws InterruptedException {
    Stoppable stopperForGroup1 = new SampleStopper();
    Stoppable stopperForGroup2 = new SampleStopper();
    final int period = 100;
    final int delta = period / 10;
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
  }

  @Test
  public void testShutdownCancelsScheduledChores() throws InterruptedException {
    final int period = 100;
    ScheduledChore successChore1 = new DoNothingChore("sc1", period);
    ScheduledChore successChore2 = new DoNothingChore("sc2", period);
    ScheduledChore successChore3 = new DoNothingChore("sc3", period);
    assertTrue(service.scheduleChore(successChore1));
    assertTrue(successChore1.isScheduled());
    assertTrue(service.scheduleChore(successChore2));
    assertTrue(successChore2.isScheduled());
    assertTrue(service.scheduleChore(successChore3));
    assertTrue(successChore3.isScheduled());

    shutdownService(service);

    assertFalse(successChore1.isScheduled());
    assertFalse(successChore2.isScheduled());
    assertFalse(successChore3.isScheduled());
  }

  @Test
  public void testShutdownWorksWhileChoresAreExecuting() throws InterruptedException {
    final int period = 100;
    final int sleep = 5 * period;
    ScheduledChore slowChore1 = new SleepingChore("sc1", period, sleep);
    ScheduledChore slowChore2 = new SleepingChore("sc2", period, sleep);
    ScheduledChore slowChore3 = new SleepingChore("sc3", period, sleep);
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
  }

  @Test
  public void testShutdownRejectsNewSchedules() throws InterruptedException {
    final int period = 100;
    ScheduledChore successChore1 = new DoNothingChore("sc1", period);
    ScheduledChore successChore2 = new DoNothingChore("sc2", period);
    ScheduledChore successChore3 = new DoNothingChore("sc3", period);
    ScheduledChore failChore1 = new DoNothingChore("fc1", period);
    ScheduledChore failChore2 = new DoNothingChore("fc2", period);
    ScheduledChore failChore3 = new DoNothingChore("fc3", period);

    assertTrue(service.scheduleChore(successChore1));
    assertTrue(successChore1.isScheduled());
    assertTrue(service.scheduleChore(successChore2));
    assertTrue(successChore2.isScheduled());
    assertTrue(service.scheduleChore(successChore3));
    assertTrue(successChore3.isScheduled());

    shutdownService(service);

    assertFalse(service.scheduleChore(failChore1));
    assertFalse(failChore1.isScheduled());
    assertFalse(service.scheduleChore(failChore2));
    assertFalse(failChore2.isScheduled());
    assertFalse(service.scheduleChore(failChore3));
    assertFalse(failChore3.isScheduled());
  }

  /**
   * for HBASE-25014
   */
  @Test
  public void testInitialDelay() {
    SampleStopper stopper = new SampleStopper();
    service.scheduleChore(new ScheduledChore("chore", stopper, 1000, 2000) {
      @Override
      protected void chore() {
        stopper.stop("test");
      }
    });
    Waiter.waitFor(CONF, 5000, () -> stopper.isStopped());
  }

  @Test
  public void testCleanupWithStopper() {
    SampleStopper stopper = new SampleStopper();
    DoNothingChore chore = spy(new DoNothingChore("chore", stopper, 10));
    service.scheduleChore(chore);
    assertTrue(chore.isScheduled());
    verify(chore, never()).cleanup();
    stopper.stop("test");
    Waiter.waitFor(CONF, 200, () -> !chore.isScheduled());
    verify(chore, atLeastOnce()).cleanup();
  }

  @Test
  public void testCleanupWithShutdown() {
    DoNothingChore chore = spy(new DoNothingChore("chore", 10));
    service.scheduleChore(chore);
    assertTrue(chore.isScheduled());
    verify(chore, never()).cleanup();
    chore.shutdown(true);
    Waiter.waitFor(CONF, 200, () -> !chore.isScheduled());
    verify(chore, atLeastOnce()).cleanup();
  }
}
