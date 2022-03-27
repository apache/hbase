/*
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
package org.apache.hadoop.hbase.util.clock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class InstrumentedIncrementAdvancingClock extends IncrementAdvancingClock {

  static final Logger LOG = LoggerFactory.getLogger(InstrumentedIncrementAdvancingClock.class);
  final LongAdder countOk = new LongAdder();
  final LongAdder countAdvance = new LongAdder();
  final LongAdder currentAdvance = new LongAdder();
  final AtomicLong maxAdvance = new AtomicLong();

  public InstrumentedIncrementAdvancingClock(HashedBytes name) {
    super(name);
  }

  @Override
  protected long advance(long last) {
    countAdvance.increment();
    currentAdvance.increment();
    return super.advance(last);
  }

  @Override
  protected long update(long now) {
    countOk.increment();
    maxAdvance.updateAndGet(u -> Math.max(u, currentAdvance.longValue()));
    currentAdvance.reset();
    return super.update(now);
  }

  @Override
  public boolean remove() {
    boolean result = super.remove();
    LOG.debug("{}: ok={}, advanced={}, maxAdvance={}", getName(), countOk.longValue(),
      countAdvance.longValue(), maxAdvance.get());
    return result;
  }

}
