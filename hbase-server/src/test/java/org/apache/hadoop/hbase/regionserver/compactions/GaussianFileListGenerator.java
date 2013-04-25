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

import org.apache.commons.math.random.GaussianRandomGenerator;
import org.apache.commons.math.random.MersenneTwister;
import org.apache.hadoop.hbase.regionserver.StoreFile;

class GaussianFileListGenerator extends StoreFileListGenerator {

  GaussianFileListGenerator() {
    super(GaussianFileListGenerator.class);
  }

  @Override
  public Iterator<List<StoreFile>> iterator() {
    return new Iterator<List<StoreFile>>() {
      private GaussianRandomGenerator gen =
          new GaussianRandomGenerator(new MersenneTwister(random.nextInt()));
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < MAX_FILE_GEN_ITERS;
      }

      @Override
      public List<StoreFile> next() {
        count += 1;
        ArrayList<StoreFile> files = new ArrayList<StoreFile>(NUM_FILES_GEN);
        for (int i = 0; i < NUM_FILES_GEN; i++) {
          files.add(createMockStoreFile(
              (int) Math.ceil(Math.max(0, gen.nextNormalizedDouble() * 32 + 32)))
          );
        }

        return files;
      }

      @Override
      public void remove() {
      }
    };
  }
}
