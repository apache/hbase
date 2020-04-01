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

import org.apache.hadoop.fs.CanUnbuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

public class TestFSDataInputStreamWrapper {
    private Method unbuffer = null;

    @Test
    public void unbuffer() {
        InputStream wrappedStream = new SonStream();
        final Class<? extends InputStream> streamClass = wrappedStream.getClass();
        Class<?>[] streamInterfaces = streamClass.getInterfaces();
        for (Class c : streamInterfaces) {
            if (c.getCanonicalName().toString().equals("org.apache.hadoop.fs.CanUnbuffer")) {
                try {
                    this.unbuffer = streamClass.getDeclaredMethod("unbuffer");
                } catch (NoSuchMethodException | SecurityException e) {
                    return;
                }
                break;
            }
        }
        Assert.assertEquals(false, unbuffer != null);
        unbuffer = null;
        if (wrappedStream instanceof CanUnbuffer) {
            try {
                this.unbuffer = streamClass.getDeclaredMethod("unbuffer");
            } catch (NoSuchMethodException | SecurityException e) {
                return;
            }
        }
        Assert.assertEquals(true, unbuffer != null);
    }

    public class SonStream extends FatherStream {
        @Override
        public void unbuffer() {

        }
    }

    public class FatherStream extends InputStream implements CanUnbuffer {

        @Override
        public void unbuffer() {

        }

        @Override
        public int read() throws IOException {
            return 0;
        }
    }
}
