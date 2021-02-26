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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Mark a class that it has a RegionServiceServices accessor.
 * Temporary hack until core Coprocesssors are integrated.
 * @see CoreCoprocessor
 * @deprecated Since 2.0.0 to be removed in 3.0.0. The hope is that by 3.0.0 we will not need this
 * facility as CoreCoprocessors are integated into core.
 */
@Deprecated
@InterfaceAudience.Private
public interface HasRegionServerServices {
  /**
   * @return An instance of RegionServerServices, an object NOT for Coprocessor consumption.
   */
  RegionServerServices getRegionServerServices();
}
