/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

@Category({IOTests.class, SmallTests.class})
public class TestFSDataInputStreamWrapper {
    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static FileSystem fs;

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestFSDataInputStreamWrapper.class);

    @BeforeClass
    public static void setUp() throws Exception {
        fs = TEST_UTIL.getTestFileSystem();
    }

    @Test
    public void TestUnbuffer() throws IOException {
        String testDir = "/tmp/";
        String testFile = "test.txt";
        createFile(testDir, testFile);
        SonStream stream = new SonStream(fs.open(new Path(testDir + testFile)));
        FSDataInputStreamWrapper fsDISW = new FSDataInputStreamWrapper(stream);
        fsDISW.unbuffer();
        Assert.assertEquals(stream.buffer, null);
    }

    public class SonStream extends FSDataInputStream {

        public ByteBuffer buffer = ByteBuffer.allocate(1024);

        public SonStream(InputStream in) {
            super(in);
        }

        public InputStream getWrappedStream() {
            return this;
        }

        @Override
        public void unbuffer() {
            buffer = null;
        }
    }

    public static void createFile(String directory, String filename) {
        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
        }
        File file2 = new File(directory, filename);
        if (!file2.exists()) {
            try {
                file2.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
