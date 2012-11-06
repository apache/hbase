/**
 * Copyright The Apache Software Foundation.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.hbase.regionserver.compactionhook;

import org.apache.hadoop.hbase.regionserver.RestrictedKeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class SkipCompactionHook implements CompactionHook {

  /**
   * Returns null only if the value of the KV contains 'c' or 'C' in its String
   * representation
   */
  @Override
  public RestrictedKeyValue transform(RestrictedKeyValue kv) {
    String currentValue = Bytes.toString(kv.getValue());
    if (currentValue.contains("C") || currentValue.contains("c")) {
      return null;
    }
    return kv;
  }
}
