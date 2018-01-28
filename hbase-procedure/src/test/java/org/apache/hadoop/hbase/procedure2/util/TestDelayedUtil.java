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
package org.apache.hadoop.hbase.procedure2.util;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestDelayedUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDelayedUtil.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDelayedUtil.class);

  @Test
  public void testDelayedContainerEquals() {
    Object o1 = new Object();
    Object o2 = new Object();
    ZeroDelayContainer<Long> lnull = new ZeroDelayContainer(null);
    ZeroDelayContainer<Long> l10a = new ZeroDelayContainer<>(10L);
    ZeroDelayContainer<Long> l10b = new ZeroDelayContainer(10L);
    ZeroDelayContainer<Long> l15 = new ZeroDelayContainer(15L);
    ZeroDelayContainer<Object> onull = new ZeroDelayContainer<>(null);
    ZeroDelayContainer<Object> o1ca = new ZeroDelayContainer<>(o1);
    ZeroDelayContainer<Object> o1cb = new ZeroDelayContainer<>(o1);
    ZeroDelayContainer<Object> o2c = new ZeroDelayContainer<>(o2);

    ZeroDelayContainer[] items = new ZeroDelayContainer[] {
      lnull, l10a, l10b, l15, onull, o1ca, o1cb, o2c,
    };

    assertContainersEquals(lnull, items, lnull, onull);
    assertContainersEquals(l10a, items, l10a, l10b);
    assertContainersEquals(l10b, items, l10a, l10b);
    assertContainersEquals(l15, items, l15);
    assertContainersEquals(onull, items, lnull, onull);
    assertContainersEquals(o1ca, items, o1ca, o1cb);
    assertContainersEquals(o1cb, items, o1ca, o1cb);
    assertContainersEquals(o2c, items, o2c);
  }

  private void assertContainersEquals(final ZeroDelayContainer src,
      final ZeroDelayContainer[] items, final ZeroDelayContainer... matches) {
    for (int i = 0; i < items.length; ++i) {
      boolean shouldMatch = false;
      for (int j = 0; j < matches.length; ++j) {
        if (items[i] == matches[j]) {
          shouldMatch = true;
          break;
        }
      }
      boolean isMatching = src.equals(items[i]);
      assertEquals(src.getObject() + " unexpectedly match " + items[i].getObject(),
        shouldMatch, isMatching);
    }
  }

  private static class ZeroDelayContainer<T> extends DelayedUtil.DelayedContainer<T> {
    public ZeroDelayContainer(final T object) {
      super(object);
    }

    @Override
    public long getTimeout() {
      return 0;
    }
  }
}
