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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Class to create list of mock storefiles of specified length.
 * This is great for testing edge cases.
 */
class ExplicitFileListGenerator extends StoreFileListGenerator {
  /** The explicit files size lists to return. */
  private int[][] fileSizes = new int[][]{
      {1000, 350, 200, 100, 20, 10, 10},
      {1000, 450, 200, 100, 20, 10, 10},
      {1000, 550, 200, 100, 20, 10, 10},
      {1000, 650, 200, 100, 20, 10, 10},
      {1, 1, 600, 1, 1, 1, 1},
      {1, 1, 600, 600, 600, 600, 600, 1, 1, 1, 1},
      {1, 1, 600, 600, 600, 1, 1, 1, 1},
      {1000, 250, 25, 25, 25, 25, 25, 25},
      {25, 25, 25, 25, 25, 25, 500},
      {1000, 1000, 1000, 1000, 900},
      {107, 50, 10, 10, 10, 10},
      {2000, 107, 50, 10, 10, 10, 10},
      {9, 8, 7, 6, 5, 4, 3, 2, 1},
      {11, 18, 9, 8, 7, 6, 5, 4, 3, 2, 1},
      {110, 18, 18, 18, 18, 9, 8, 7, 6, 5, 4, 3, 2, 1},
      {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15}
  };

  ExplicitFileListGenerator() {
    super(ExplicitFileListGenerator.class);
  }

  @Override
  public final Iterator<List<StoreFile>> iterator() {
    return new Iterator<List<StoreFile>>() {
      private int nextIndex = 0;
      @Override
      public boolean hasNext() {
        return nextIndex < fileSizes.length;
      }

      @Override
      public List<StoreFile> next() {
        List<StoreFile> files =  createStoreFileList(fileSizes[nextIndex]);
        nextIndex += 1;
        return files;
      }

      @Override
      public void remove() {
      }
    };
  }
}
