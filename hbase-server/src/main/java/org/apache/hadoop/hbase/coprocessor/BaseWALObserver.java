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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * An abstract class that implements WALObserver.
 * By extending it, you can create your own WAL observer without
 * overriding all abstract methods of WALObserver.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class BaseWALObserver implements WALObserver {
  @Override
  public void start(CoprocessorEnvironment e) throws IOException { }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException { }

  /**
   * Implementers should override this method and leave the deprecated version as-is.
   */
  @Override
  public boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    return false;
  }

  /**
   * Implementers should override this method and leave the deprecated version as-is.
   */
  @Override
  public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {}

  @Override
  public void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException { }

  @Override
  public void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException { }
}
