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
package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Internal scanners differ from client-side scanners in that they operate on HStoreKeys and byte[]
 * instead of RowResults. This is because they are actually close to how the data is physically
 * stored, and therefore it is more convenient to interact with them that way. It is also much
 * easier to merge the results across SortedMaps than RowResults.
 * <p>
 * Additionally, we need to be able to determine if the scanner is doing wildcard column matches
 * (when only a column family is specified or if a column regex is specified) or if multiple members
 * of the same column family were specified. If so, we need to ignore the timestamp to ensure that
 * we get all the family members, as they may have been last updated at different times.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface InternalScanner extends Closeable {
  /**
   * Grab the next row's worth of values.
   * @param result return output array
   * @return true if more rows exist after this one, false if scanner is done
   * @throws IOException e
   */
  default boolean next(List<Cell> result) throws IOException {
    return next(result, NoLimitScannerContext.getInstance());
  }

  /**
   * Grab the next row's worth of values.
   * @param result return output array n * @return true if more rows exist after this one, false if
   *               scanner is done
   * @throws IOException e
   */
  boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException;

  /**
   * Closes the scanner and releases any resources it has allocated n
   */
  @Override
  void close() throws IOException;
}
