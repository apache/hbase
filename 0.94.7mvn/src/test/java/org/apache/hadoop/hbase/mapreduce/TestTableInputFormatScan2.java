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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestTableInputFormatScan part 2.
 * @see TestTableInputFormatScanBase
 */
@Category(LargeTests.class)
public class TestTableInputFormatScan2 extends TestTableInputFormatScanBase {

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToQPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "qpp", "qpo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOPPToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("opp", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYXToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyx", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyy", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYZYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }

  @Test
  public void testScanFromConfiguration()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScanFromConfiguration("bba", "bbd", "bbc");
  }
}
