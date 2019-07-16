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
package org.apache.hadoop.hbase.util.hbck;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * @deprecated Since 2.0.0. Will be removed in 3.0.0. We've deprecated this tool in hbase-2+
 *             because it destroyed the hbase2 meta table.
 */
@Deprecated
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public final class OfflineMetaRepair {

  // Private constructor included here to avoid checkstyle warnings
  private OfflineMetaRepair() {}

  public static void main(String[] args) throws Exception {
    System.err.println("This tool is no longer supported in HBase-2+."
      + " Please refer to https://hbase.apache.org/book.html#HBCK2");
    System.exit(1);
  }
}
