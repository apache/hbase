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

/**
 * <p>
 * This interface should be implemented for compaction hooks. The implementation
 * should be stateless, because a common instance of the implementation might be
 * reused across multiple active compactions.To see an example of a compaction
 * hook implementation go here: {@link LowerToUpperCompactionHook}.
 * </p>
 */

public interface CompactionHook {

  /**
   * This should be called to transform a KeyValue's value into a desired
   * format. You can do the following transformations:
   * <ul>
   * <li>return null: that way you will skip this KV </li>
   * <li>return the same kv: if you don't want to apply any changes to the KV</li>
   * <li> transform only the value of the KV into the desired format</li>
   * </ul>
   *
   * @param kv - the KeyValueForCompactionHooks that we want to transform
   * @return the resulting KeyValueForCompactionHooks
   */
  RestrictedKeyValue transform(RestrictedKeyValue kv);

}
