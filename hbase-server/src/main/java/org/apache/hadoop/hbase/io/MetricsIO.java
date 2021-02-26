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

package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsIO {

  private final MetricsIOSource source;
  private final MetricsIOWrapper wrapper;

  public MetricsIO(MetricsIOWrapper wrapper) {
    this(CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .createIO(wrapper), wrapper);
  }

  MetricsIO(MetricsIOSource source, MetricsIOWrapper wrapper) {
    this.source = source;
    this.wrapper = wrapper;
  }

  public MetricsIOSource getMetricsSource() {
    return source;
  }

  public MetricsIOWrapper getWrapper() {
    return wrapper;
  }

  public void updateFsReadTime(long t) {
    source.updateFsReadTime(t);
  }

  public void updateFsPreadTime(long t) {
    source.updateFsPReadTime(t);
  }

  public void updateFsWriteTime(long t) {
    source.updateFsWriteTime(t);
  }
}
