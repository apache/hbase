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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MetricsTests;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({MetricsTests.class, SmallTests.class})
public class TestCompatibilitySingletonFactory {

  private static final int ITERATIONS = 100000;
  private static final Random RANDOM = new Random();

  private class TestCompatibilitySingletonFactoryCallable implements Callable<String> {

    @Override
    public String call() throws Exception {
      Thread.sleep(RANDOM.nextInt(10));
      RandomStringGenerator
          instance =
          CompatibilitySingletonFactory.getInstance(RandomStringGenerator.class);
      return instance.getRandString();
    }
  }

  @Test
  public void testGetInstance() throws Exception {
    List<TestCompatibilitySingletonFactoryCallable> callables =
        new ArrayList<TestCompatibilitySingletonFactoryCallable>(ITERATIONS);
    List<String> resultStrings = new ArrayList<String>(ITERATIONS);


    // Create the callables.
    for (int i = 0; i < ITERATIONS; i++) {
      callables.add(new TestCompatibilitySingletonFactoryCallable());
    }

    // Now run the callables.
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    List<Future<String>> futures = executorService.invokeAll(callables);

    // Wait for them all to finish.
    for (Future<String> f : futures) {
      resultStrings.add(f.get());
    }

    // Get the first string.
    String firstString = resultStrings.get(0);


    // Assert that all the strings are equal to the fist.
    for (String s : resultStrings) {
      assertEquals(firstString, s);
    }

    // an assert to make sure that RandomStringGeneratorImpl is generating random strings.
    assertNotEquals(new RandomStringGeneratorImpl().getRandString(), firstString);
  }
}
