/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 *  Builder implementation to create {@link Tag}
 *  Call setTagValue(byte[]) method to create {@link ArrayBackedTag}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface TagBuilder {
  /**
   * Set type of the tag.
   * @param tagType type of the tag
   * @return {@link TagBuilder}
   */
  TagBuilder setTagType(byte tagType);

  /**
   * Set the value of the tag.
   * @param tagBytes tag bytes.
   * @return {@link TagBuilder}
   */
  TagBuilder setTagValue(byte[] tagBytes);

  /**
   * Build the tag.
   * @return {@link Tag}
   */
  Tag build();
}
