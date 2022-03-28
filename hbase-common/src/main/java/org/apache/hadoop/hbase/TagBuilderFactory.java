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
 * Factory to create Tags.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public final class TagBuilderFactory {

  public static TagBuilder create() {
    return new TagBuilderImpl();
  }
}

/**
 * Builder implementation to create {@link Tag}<br>
 * Call setTagValue(byte[]) method to create {@link ArrayBackedTag}
 */
class TagBuilderImpl implements TagBuilder {
  // This assumes that we never create tag with value less than 0.
  private byte tagType = (byte)-1;
  private byte[] tagBytes = null;
  public static final String TAG_TYPE_NOT_SET_EXCEPTION = "Need to set type of the tag.";
  public static final String TAG_VALUE_NULL_EXCEPTION = "TagBytes can't be null";

  @Override
  public TagBuilder setTagType(byte tagType) {
    this.tagType = tagType;
    return this;
  }

  @Override
  public TagBuilder setTagValue(byte[] tagBytes) {
    this.tagBytes = tagBytes;
    return this;
  }

  private void validate() {
    if (tagType == -1) {
      throw new IllegalArgumentException(TAG_TYPE_NOT_SET_EXCEPTION);
    }
    if (tagBytes == null) {
      throw new IllegalArgumentException(TAG_VALUE_NULL_EXCEPTION);
    }
  }

  @Override
  public Tag build() {
    validate();
    return new ArrayBackedTag(tagType, tagBytes);
  }
}
