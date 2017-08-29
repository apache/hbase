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

package org.apache.hadoop.hbase.codec.prefixtree.encode;

import java.io.OutputStream;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Retrieve PrefixTreeEncoders from this factory which handles pooling them and preparing the
 * ones retrieved from the pool for usage.
 */
@InterfaceAudience.Private
public class EncoderFactory {

  private static final EncoderPool POOL = new EncoderPoolImpl();


  public static PrefixTreeEncoder checkOut(OutputStream outputStream, boolean includeMvccVersion) {
    return POOL.checkOut(outputStream, includeMvccVersion);
  }

  public static void checkIn(PrefixTreeEncoder encoder) {
    POOL.checkIn(encoder);
  }


  /**************************** helper ******************************/

  protected static PrefixTreeEncoder prepareEncoder(PrefixTreeEncoder encoder,
      OutputStream outputStream, boolean includeMvccVersion) {
    PrefixTreeEncoder ret = encoder;
    if (encoder == null) {
      ret = new PrefixTreeEncoder(outputStream, includeMvccVersion);
    }
    ret.reset(outputStream, includeMvccVersion);
    return ret;
  }

}
