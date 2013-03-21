package org.apache.hadoop.hbase.util;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * Run unit test until it fails or MAX_RUN_COUNT times
 * http://stackoverflow.com/questions
 * /1835523/is-there-a-way-to-make-eclipse-run-
 * a-junit-test-multiple-times-until-failure
 *
 *
 */
public class UnitTestRunner extends Runner {

  private static final int MAX_RUN_COUNT = 50;

  private BlockJUnit4ClassRunner runner;

  @SuppressWarnings("rawtypes")
  public UnitTestRunner(final Class testClass) throws InitializationError {
    runner = new BlockJUnit4ClassRunner(testClass);
  }

  @Override
  public Description getDescription() {
    final Description description = Description
        .createSuiteDescription("Run many times until failure");
    description.addChild(runner.getDescription());
    return description;
  }

  @Override
  public void run(final RunNotifier notifier) {
    class L extends RunListener {
      boolean shouldContinue = true;
      int runCount = 0;

      @Override
      public void testFailure(final Failure failure) throws Exception {
        shouldContinue = false;
      }

      @Override
      public void testFinished(Description description) throws Exception {
        runCount++;
        shouldContinue = (shouldContinue && runCount < MAX_RUN_COUNT);
      }
    }

    final L listener = new L();
    notifier.addListener(listener);

    while (listener.shouldContinue) {
      runner.run(notifier);
    }
  }
}
