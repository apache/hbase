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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.HStoreFile;

class SinusoidalFileListGenerator extends StoreFileListGenerator {

  @Override
  public Iterator<List<HStoreFile>> iterator() {
    return new Iterator<List<HStoreFile>>() {
      private int count = 0;
      @Override
      public boolean hasNext() {
        return count < MAX_FILE_GEN_ITERS;
      }

      @Override
      public List<HStoreFile> next() {
        count += 1;
        ArrayList<HStoreFile> files = new ArrayList<>(NUM_FILES_GEN);
        for (int x = 0; x < NUM_FILES_GEN; x++) {
          int fileSize = (int) Math.abs(64 * Math.sin((Math.PI * x) / 50.0)) + 1;
          files.add(createMockStoreFile(fileSize));
        }
        return files;
      }

      @Override
      public void remove() {
      }
    };
  }

}
