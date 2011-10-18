/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.Writer;
import org.apache.hadoop.hbase.io.HeapSize;

/**
 * An interface that exposes methods to retrieve the column type and BlockType
 * of a particular cached block. This is more information than that which is
 * required by most cache implementations, but is used for more specific
 * metrics, for example. Used by implementations of HeapSize, such as
 * {@link HFileBlock}
 */
public interface HFileBlockInfo {
  /**
   * @return Column family name of this cached item.
   */
  public String getColumnFamilyName();

  /**
   * @return BlockType descriptor of this cached item. Indicates the type of
   *         data, such as a data block or an index one.
   */
  public BlockType getBlockType();
}
