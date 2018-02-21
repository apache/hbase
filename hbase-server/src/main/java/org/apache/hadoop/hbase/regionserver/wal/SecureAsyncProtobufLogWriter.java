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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SecureAsyncProtobufLogWriter extends AsyncProtobufLogWriter {

  private Encryptor encryptor = null;

  public SecureAsyncProtobufLogWriter(EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) {
    super(eventLoopGroup, channelClass);
  }

  /*
   * @return class name which is recognized by hbase-1.x to avoid ProtobufLogReader throwing error:
   *   IOException: Got unknown writer class: SecureAsyncProtobufLogWriter
   */
  @Override
  protected String getWriterClassName() {
    return "SecureProtobufLogWriter";
  }
  @Override
  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    return super.buildSecureWALHeader(conf, builder);
  }

  @Override
  protected void setEncryptor(Encryptor encryptor) {
    this.encryptor = encryptor;
  }

  @Override
  protected void initAfterHeader(boolean doCompress) throws IOException {
    super.secureInitAfterHeader(doCompress, encryptor);
  }
}
