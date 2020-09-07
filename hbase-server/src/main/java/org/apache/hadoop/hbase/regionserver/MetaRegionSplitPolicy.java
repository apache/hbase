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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The split policy for meta.
 * <p/>
 * Now we just use {@link DelimitedKeyPrefixRegionSplitPolicy} with
 * {@value org.apache.hadoop.hbase.HConstants#DELIMITER}, which means all the records for a table
 * will be in the same region, so the multi-mutate operation when splitting/merging is still valid.
 */
@InterfaceAudience.Private
public class MetaRegionSplitPolicy extends DelimitedKeyPrefixRegionSplitPolicy {

  @Override
  protected void configureForRegion(HRegion region) {
    // TODO: it will issue an error of can not find the delimiter
    super.configureForRegion(region);
    delimiter = Bytes.toBytes(HConstants.DELIMITER);
  }
}
