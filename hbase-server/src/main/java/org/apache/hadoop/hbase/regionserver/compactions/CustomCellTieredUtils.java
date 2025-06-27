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

import static org.apache.hadoop.hbase.regionserver.StoreEngine.STORE_ENGINE_CLASS_KEY;
import static org.apache.hadoop.hbase.regionserver.compactions.CustomCellTieringValueProvider.TIERING_CELL_QUALIFIER;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CustomCellTieredUtils {

  public static void checkForModifyTable(TableDescriptor newTable) throws IOException {
    for (ColumnFamilyDescriptor descriptor : newTable.getColumnFamilies()) {
      String storeEngineClassName = descriptor.getConfigurationValue(STORE_ENGINE_CLASS_KEY);
      if (
        storeEngineClassName != null && storeEngineClassName.contains("CustomCellTieredStoreEngine")
      ) {
        if (descriptor.getConfigurationValue(TIERING_CELL_QUALIFIER) == null) {
          throw new DoNotRetryIOException("StoreEngine " + storeEngineClassName
            + " is missing required " + TIERING_CELL_QUALIFIER + " parameter.");
        }
      }
    }
  }

}
