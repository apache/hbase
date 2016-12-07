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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.regionserver.HRegion.ObservedExceptionsInBatch;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link ObservedExceptionsInBatch}.
 */
@Category(SmallTests.class)
public class TestObservedExceptionsInBatch {

  private ObservedExceptionsInBatch observedExceptions;

  @Before
  public void setup() {
    observedExceptions = new ObservedExceptionsInBatch();
  }

  @Test
  public void testNoObservationsOnCreation() {
    assertFalse(observedExceptions.hasSeenFailedSanityCheck());
    assertFalse(observedExceptions.hasSeenNoSuchFamily());
    assertFalse(observedExceptions.hasSeenWrongRegion());
  }

  @Test
  public void testObservedAfterRecording() {
    observedExceptions.sawFailedSanityCheck();
    assertTrue(observedExceptions.hasSeenFailedSanityCheck());
    observedExceptions.sawNoSuchFamily();
    assertTrue(observedExceptions.hasSeenNoSuchFamily());
    observedExceptions.sawWrongRegion();
    assertTrue(observedExceptions.hasSeenWrongRegion());
  }
}
