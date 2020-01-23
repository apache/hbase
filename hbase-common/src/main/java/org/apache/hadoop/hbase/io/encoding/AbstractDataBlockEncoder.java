/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractDataBlockEncoder implements DataBlockEncoder {

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      DataBlockEncoding encoding, byte[] header, HFileContext meta) {
    return new HFileBlockDefaultEncodingContext(encoding, header, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

  protected void postEncoding(HFileBlockEncodingContext encodingCtx)
      throws IOException {
    if (encodingCtx.getDataBlockEncoding() != DataBlockEncoding.NONE) {
      encodingCtx.postEncoding(BlockType.ENCODED_DATA);
    } else {
      encodingCtx.postEncoding(BlockType.DATA);
    }
  }

  protected Cell createFirstKeyCell(ByteBuffer key, int keyLength) {
    if (key.hasArray()) {
      return new KeyValue.KeyOnlyKeyValue(key.array(), key.arrayOffset()
          + key.position(), keyLength);
    } else {
      return new ByteBufferKeyOnlyKeyValue(key, key.position(), keyLength);
    }
  }

  /**
   * Decorates EncodedSeeker with a {@link HFileBlockDecodingContext}
   */
  protected abstract static class AbstractEncodedSeeker implements EncodedSeeker {
    protected HFileBlockDecodingContext decodingCtx;

    public AbstractEncodedSeeker(HFileBlockDecodingContext decodingCtx) {
      this.decodingCtx = decodingCtx;
    }

    protected boolean includesMvcc() {
      return this.decodingCtx.getHFileContext().isIncludesMvcc();
    }

    protected boolean includesTags() {
      return this.decodingCtx.getHFileContext().isIncludesTags();
    }
  }
}
