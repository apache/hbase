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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestHRegion with hbase.busy.wait.duration set to 1000 (1 second).
 * We can't use parameterized test since TestHRegion is old fashion.
 */
@Category(MediumTests.class)
public class TestHRegionBusyWait extends TestHRegion {
  // TODO: This subclass runs all the tests in TestHRegion as well as the test below which means
  // all TestHRegion tests are run twice.
  @Before
  public void setup() throws IOException {
    super.setup();
    conf.set("hbase.busy.wait.duration", "1000");
  }

  /**
   * Test RegionTooBusyException thrown when region is busy
   */
  @Test (timeout=2000)
  public void testRegionTooBusy() throws IOException {
    String method = "testRegionTooBusy";
    byte[] tableName = Bytes.toBytes(method);
    byte[] family = Bytes.toBytes("family");
    region = initHRegion(tableName, method, conf, family);
    final AtomicBoolean stopped = new AtomicBoolean(true);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          region.lock.writeLock().lock();
          stopped.set(false);
          while (!stopped.get()) {
            Thread.sleep(100);
          }
        } catch (InterruptedException ie) {
        } finally {
          region.lock.writeLock().unlock();
        }
      }
    });
    t.start();
    Get get = new Get(row);
    try {
      while (stopped.get()) {
        Thread.sleep(100);
      }
      region.get(get);
      fail("Should throw RegionTooBusyException");
    } catch (InterruptedException ie) {
      fail("test interrupted");
    } catch (RegionTooBusyException e) {
      // Good, expected
    } finally {
      stopped.set(true);
      try {
        t.join();
      } catch (Throwable e) {
      }

      HRegion.closeHRegion(region);
      region = null;
    }
  }
}