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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.wal.WALEdit;

import com.google.protobuf.Message;

/**
 * Base class for RowProcessor with some default implementations.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public abstract class BaseRowProcessor<S extends Message,T extends Message> 
implements RowProcessor<S,T> {

  @Override
  public void preProcess(HRegion region, WALEdit walEdit) throws IOException {
  }

  @Override
  public void preBatchMutate(HRegion region, WALEdit walEdit) throws IOException {
  }

  @Override
  public void postBatchMutate(HRegion region) throws IOException {
  }

  @Override
  public void postProcess(HRegion region, WALEdit walEdit, boolean success) throws IOException {
  }

  @Override
  public List<UUID> getClusterIds() {
    return Collections.emptyList();
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName().toLowerCase(Locale.ROOT);
  }

  @Override
  public Durability useDurability() {
    return Durability.USE_DEFAULT;
  }
}
