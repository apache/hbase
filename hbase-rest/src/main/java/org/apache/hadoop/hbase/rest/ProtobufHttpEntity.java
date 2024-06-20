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
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For sending a Protobuf encoded object via Apache HttpClient efficiently without an interim byte
 * array. This exposes the underlying Apache HttpClient types, but so do the other client classes.
 */
@InterfaceAudience.Public
public class ProtobufHttpEntity implements HttpEntity {

  private ProtobufMessageHandler handler;

  public ProtobufHttpEntity(ProtobufMessageHandler handler) {
    this.handler = handler;
  }

  @Override
  public boolean isRepeatable() {
    return false;
  }

  @Override
  public boolean isChunked() {
    return true;
  }

  @Override
  public long getContentLength() {
    return -1;
  }

  @Override
  public Header getContentType() {
    return new BasicHeader(HTTP.CONTENT_TYPE, Constants.MIMETYPE_PROTOBUF_IETF);
  }

  @Override
  public Header getContentEncoding() {
    return null;
  }

  @Override
  public InputStream getContent() throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException("only writeTo is supported");
  }

  @Override
  public void writeTo(OutputStream outStream) throws IOException {
    handler.writeProtobufOutput(outStream);
  }

  @Override
  public boolean isStreaming() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void consumeContent() throws IOException {
  }

}
