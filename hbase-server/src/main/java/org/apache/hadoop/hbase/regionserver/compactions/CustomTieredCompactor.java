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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.DateTieredMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CustomTieredCompactor extends DateTieredCompactor {

  public static final String TIERING_VALUE_PROVIDER =
    "hbase.hstore.custom-tiering-value.provider.class";
  private TieringValueProvider tieringValueProvider;

  public CustomTieredCompactor(Configuration conf, HStore store) throws IOException {
    super(conf, store);
    String className =
      conf.get(TIERING_VALUE_PROVIDER, CustomCellTieringValueProvider.class.getName());
    try {
      tieringValueProvider =
        (TieringValueProvider) Class.forName(className).getConstructor().newInstance();
      tieringValueProvider.init(conf);
    } catch (Exception e) {
      throw new IOException("Unable to load configured tiering value provider '" + className + "'",
        e);
    }
  }

  @Override
  protected List<ExtendedCell> decorateCells(List<ExtendedCell> cells) {
    return tieringValueProvider.decorateCells(cells);
  }

  @Override
  protected DateTieredMultiFileWriter createMultiWriter(final CompactionRequestImpl request,
    final List<Long> lowerBoundaries, final Map<Long, String> lowerBoundariesPolicies) {
    return new CustomTieringMultiFileWriter(lowerBoundaries, lowerBoundariesPolicies,
      needEmptyFile(request), CustomTieredCompactor.this.tieringValueProvider::getTieringValue);
  }

  public interface TieringValueProvider {

    void init(Configuration configuration) throws Exception;

    default List<ExtendedCell> decorateCells(List<ExtendedCell> cells) {
      return cells;
    }

    long getTieringValue(ExtendedCell cell);
  }

}
