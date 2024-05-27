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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * Common interface for models capable of supporting protobuf marshalling and unmarshalling. Hooks
 * up to the ProtobufMessageBodyConsumer and ProtobufMessageBodyProducer adapters.
 */
@InterfaceAudience.Private
public interface ProtobufMessageHandler {

  // The Jetty 9.4 HttpOutput default commit size is 32K/4 = 8K. We use that size to avoid
  // double buffering (and copying) in HttpOutput. If we ever increase the HttpOutput commit size,
  // we need to adjust this accordingly. We should also revisit this when Jetty is upgraded.
  int BUFFER_SIZE = 8 * 1024;

  /** Writes the protobuf represention of the model to os */
  default void writeProtobufOutput(OutputStream os) throws IOException {
    // Creating an explicit CodedOutputStream for the following reasons :
    // 1. This avoids the cost of pre-computing the message size
    // 2. This lets us set the buffer size explicitly
    CodedOutputStream cos = CodedOutputStream.newInstance(os, BUFFER_SIZE);
    messageFromObject().writeTo(cos);
    cos.flush();
  }

  /**
   * Use {@link org.apache.hadoop.hbase.rest.ProtobufMessageHandler#writeProtobufOutput(OutputStream)} for better performance
   * @return the protobuf encoded object in a byte array
   */
  default byte[] createProtobufOutput() {
    return messageFromObject().toByteArray();
  }

  Message messageFromObject();

  /**
   * Initialize the model from a protobuf representation.
   * @param message the raw bytes of the protobuf message
   * @return reference to self for convenience
   */
  // TODO implement proper stream handling for unmarshalling.
  // Using byte array here lets us use ProtobufUtil.mergeFrom in the implementations to
  // avoid the CodedOutputStream size limitation, but is slow
  // and memory intensive. We should stream the input, and
  // provide a configuration property and a method parameter to
  // set the maximum message size
  ProtobufMessageHandler getObjectFromMessage(byte[] message) throws IOException;
}
