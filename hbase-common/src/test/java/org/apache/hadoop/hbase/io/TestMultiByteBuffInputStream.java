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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestMultiByteBuffInputStream {

  @Test
  public void testReads() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
    DataOutputStream dos = new DataOutputStream(bos);
    String s = "test";
    int i = 128;
    dos.write(1);
    dos.writeInt(i);
    dos.writeBytes(s);
    dos.writeLong(12345L);
    dos.writeShort(2);
    dos.flush();
    ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());

    // bbis contains 19 bytes
    // 1 byte, 4 bytes int, 4 bytes string, 8 bytes long and 2 bytes short
    ByteBuffInputStream bbis = new ByteBuffInputStream(new MultiByteBuff(bb));
    assertEquals(15 + s.length(), bbis.available());
    assertEquals(1, bbis.read());
    byte[] ib = new byte[4];
    bbis.read(ib);
    assertEquals(i, Bytes.toInt(ib));
    byte[] sb = new byte[s.length()];
    bbis.read(sb);
    assertEquals(s, Bytes.toString(sb));
    byte[] lb = new byte[8];
    bbis.read(lb);
    assertEquals(12345, Bytes.toLong(lb));
    assertEquals(2, bbis.available());
    ib = new byte[4];
    int read = bbis.read(ib, 0, ib.length);
    // We dont have 4 bytes remainig but only 2. So onlt those should be returned back
    assertEquals(2, read);
    assertEquals(2, Bytes.toShort(ib));
    assertEquals(0, bbis.available());
    // At end. The read() should return -1
    assertEquals(-1, bbis.read());
    bbis.close();

    bb = ByteBuffer.wrap(bos.toByteArray());
    bbis = new ByteBuffInputStream(new MultiByteBuff(bb));
    DataInputStream dis = new DataInputStream(bbis);
    dis.read();
    assertEquals(i, dis.readInt());
    dis.close();
  }
}
