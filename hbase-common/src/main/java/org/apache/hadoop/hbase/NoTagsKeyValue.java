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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An extension of the KeyValue where the tags length is always 0 
 */
@InterfaceAudience.Private
public class NoTagsKeyValue extends KeyValue {
  public NoTagsKeyValue(byte[] bytes, int offset, int length) {
    super(bytes, offset, length);
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    // In KeyValueUtil#oswrite we do a Cell serialization as KeyValue. Any changes doing here, pls
    // check KeyValueUtil#oswrite also and do necessary changes.
    writeInt(out, this.length);
    out.write(this.bytes, this.offset, this.length);
    return this.length + Bytes.SIZEOF_INT;
  }
}