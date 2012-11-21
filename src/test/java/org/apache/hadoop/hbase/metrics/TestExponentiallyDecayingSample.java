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

package org.apache.hadoop.hbase.metrics;

import junit.framework.Assert;

import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import com.yammer.metrics.stats.Snapshot;

import org.apache.hadoop.hbase.SmallTests;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestExponentiallyDecayingSample {
  
  @Test
  public void testBasic() {
      final ExponentiallyDecayingSample sample = 
          new ExponentiallyDecayingSample(100, 0.99);
      
      for (int i = 0; i < 1000; i++) {
          sample.update(i);
      }
      Assert.assertEquals(100, sample.size());
      
      final Snapshot snapshot = sample.getSnapshot();
      Assert.assertEquals(100, snapshot.size());

      for (double i : snapshot.getValues()) {
        Assert.assertTrue(i >= 0.0 && i < 1000.0);
      }
  }

  @Test
  public void testTooBig() throws Exception {
      final ExponentiallyDecayingSample sample = 
          new ExponentiallyDecayingSample(100, 0.99);
      for (int i = 0; i < 10; i++) {
          sample.update(i);
      }
      Assert.assertEquals(10, sample.size());

      final Snapshot snapshot = sample.getSnapshot();
      Assert.assertEquals(10, sample.size());

      for (double i : snapshot.getValues()) {
        Assert.assertTrue(i >= 0.0 && i < 1000.0);
      }
  }
}
