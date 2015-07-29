/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;


/**
 * This is the glue between the HRegion and whatever hadoop shim layer
 * is loaded (hbase-hadoop1-compat or hbase-hadoop2-compat).
 */
@InterfaceAudience.Private
public class MetricsRegion {
  private final MetricsRegionSource source;
  private MetricsRegionWrapper regionWrapper;

  public MetricsRegion(final MetricsRegionWrapper wrapper) {
    source = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
                                             .createRegion(wrapper);
    this.regionWrapper = wrapper;
  }

  public void close() {
    source.close();
  }

  public void updatePut() {
    source.updatePut();
  }

  public void updateDelete() {
    source.updateDelete();
  }

  public void updateGet(final long getSize) {
    source.updateGet(getSize);
  }

  public void updateScanNext(final long scanSize) {
    source.updateScan(scanSize);
  }

  public void updateAppend() {
    source.updateAppend();
  }

  public void updateIncrement() {
    source.updateIncrement();
  }

  MetricsRegionSource getSource() {
    return source;
  }

  public MetricsRegionWrapper getRegionWrapper() {
    return regionWrapper;
  }

}
