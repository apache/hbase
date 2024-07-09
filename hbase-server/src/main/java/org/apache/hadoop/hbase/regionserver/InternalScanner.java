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
import org.apache.hadoop.hbase.ExtendedCell;
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
   * <p>
   * The generic type for the output list {@code result} means we will only add {@link ExtendedCell}
   * to it. This is useful for the code in HBase as we can pass List&lt;ExtendedCell&gt; here to
   * avoid casting, but may cause some troubles for coprocessors which implement this method. In
   * general, all cells created via the {@link org.apache.hadoop.hbase.CellBuilder} are actually
   * {@link ExtendedCell}s, so if you want to add something to the {@code result} list, you can just
   * cast it to {@link ExtendedCell}, although it is marked as IA.Private.
   * @param result return output array. We will only add ExtendedCell to this list, but for CP
   *               users, you'd better just use {@link org.apache.hadoop.hbase.RawCell} as
   *               {@link ExtendedCell} is IA.Private.
   * @return true if more rows exist after this one, false if scanner is done
   */
  default boolean next(List<? super ExtendedCell> result) throws IOException {
    return next(result, NoLimitScannerContext.getInstance());
  }

  /**
   * Grab the next row's worth of values.
   * <p>
   * The generic type for the output list {@code result} means we will only add {@link ExtendedCell}
   * to it. This is useful for the code in HBase as we can pass List&lt;ExtendedCell&gt; here to
   * avoid casting, but may cause some troubles for coprocessors which implement this method. In
   * general, all cells created via the {@link org.apache.hadoop.hbase.CellBuilder} are actually
   * {@link ExtendedCell}s, so if you want to add something to the {@code result} list, you can just
   * cast it to {@link ExtendedCell}, although it is marked as IA.Private.
   * @param result return output array. We will only add ExtendedCell to this list, but for CP
   *               users, you'd better just use {@link org.apache.hadoop.hbase.RawCell} as
   *               {@link ExtendedCell} is IA.Private.
   * @return true if more rows exist after this one, false if scanner is done
   */
  boolean next(List<? super ExtendedCell> result, ScannerContext scannerContext) throws IOException;

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  @Override
  void close() throws IOException;
}
