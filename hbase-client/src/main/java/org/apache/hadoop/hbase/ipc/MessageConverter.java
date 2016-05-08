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
package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Interface to convert Messages to specific types
 * @param <M> Message Type to convert
 * @param <O> Output Type
 */
@InterfaceAudience.Private
public interface MessageConverter<M,O> {
  /**
   * Converts Message to Output
   * @param msg to convert
   * @param cellScanner to use for conversion
   * @return Output
   * @throws IOException if message could not be converted to response
   */
  O convert(M msg, CellScanner cellScanner) throws IOException;

  MessageConverter<Message,Message> NO_CONVERTER = new MessageConverter<Message, Message>() {
    @Override
    public Message convert(Message msg, CellScanner cellScanner) throws IOException {
      return msg;
    }
  };
}
