/**
 *
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
package org.apache.hadoop.hbase.util;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A collection class that contains multiple sub-lists, which allows us to not copy lists.
 * This class does not support modification. The derived classes that add modifications are
 * not thread-safe.
 * NOTE: Doesn't implement list as it is not necessary for current usage, feel free to add.
 */
@InterfaceAudience.Private
public class ConcatenatedLists<T> extends AbstractCollection<T> {
  protected final ArrayList<List<T>> components = new ArrayList<>();
  protected int size = 0;

  public void addAllSublists(List<? extends List<T>> items) {
    for (List<T> list : items) {
      addSublist(list);
    }
  }

  public void addSublist(List<T> items) {
    if (!items.isEmpty()) {
      this.components.add(items);
      this.size += items.size();
    }
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public java.util.Iterator<T> iterator() {
    return new Iterator();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification="nextWasCalled is using by StripeStoreFileManager")
  public class Iterator implements java.util.Iterator<T> {
    protected int currentComponent = 0;
    protected int indexWithinComponent = -1;
    protected boolean nextWasCalled = false;

    @Override
    public boolean hasNext() {
      return (currentComponent + 1) < components.size()
          || ((currentComponent + 1) == components.size()
              && ((indexWithinComponent + 1) < components.get(currentComponent).size()));
    }

    @Override
    public T next() {
      if (!components.isEmpty()) {
        this.nextWasCalled = true;
        List<T> src = components.get(currentComponent);
        if (++indexWithinComponent < src.size()) {
          return src.get(indexWithinComponent);
        }

        if (++currentComponent < components.size()) {
          indexWithinComponent = 0;
          src = components.get(currentComponent);
          assert src.size() > 0;
          return src.get(indexWithinComponent);
        }
      }
      this.nextWasCalled = false;
      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
