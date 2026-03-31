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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The simple version of reservoir sampling implementation. It is enough for the usage in HBase.
 * <p/>
 * See https://en.wikipedia.org/wiki/Reservoir_sampling.
 */
@InterfaceAudience.Private
public class ReservoirSample<T> {

  private final List<T> r;

  private final int k;

  private int n;

  public ReservoirSample(int k) {
    Preconditions.checkArgument(k > 0, "negative sampling number(%s) is not allowed", k);
    r = new ArrayList<>(k);
    this.k = k;
  }

  public void add(T t) {
    if (n < k) {
      r.add(t);
    } else {
      int j = ThreadLocalRandom.current().nextInt(n + 1);
      if (j < k) {
        r.set(j, t);
      }
    }
    n++;
  }

  public void add(Iterator<T> iter) {
    iter.forEachRemaining(this::add);
  }

  public void add(Stream<T> s) {
    s.forEachOrdered(this::add);
  }

  public List<T> getSamplingResult() {
    return r;
  }
}
