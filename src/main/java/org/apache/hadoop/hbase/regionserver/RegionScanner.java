/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * RegionScanner describes iterators over rows in an HRegion.
 */
public interface RegionScanner extends InternalScanner {
  /**
   * @return The RegionInfo for this scanner.
   */
  public HRegionInfo getRegionInfo();

  /**
   * @return True if a filter indicates that this scanner will return no
   *         further rows.
   */
  public boolean isFilterDone();

  /**
   * Do a reseek to the required row. Should not be used to seek to a key which
   * may come before the current position. Always seeks to the beginning of a
   * row boundary.
   *
   * @throws IOException
   * @throws IllegalArgumentException
   *           if row is null
   *
   */
  public boolean reseek(byte[] row) throws IOException;

}
