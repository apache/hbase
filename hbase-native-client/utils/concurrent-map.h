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
 *
 */

#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

namespace hbase {

/**
 * A concurrent version of std::unordered_map where we acquire a shared or exclusive
 * lock for operations. This is NOT a highly-concurrent and scalable implementation
 * since there is only one lock object.
 * Replace this with tbb::concurrent_unordered_map or similar.
 *
 * Concurrency here is different than in Java. For example, the iterators returned from
 * find() will not copy the key, value pairs.
 */
template <typename K, typename V>
class concurrent_map {
 public:
  typedef K key_type;
  typedef V mapped_type;
  typedef std::pair<const key_type, mapped_type> value_type;
  typedef typename std::unordered_map<K, V>::iterator iterator;
  typedef typename std::unordered_map<K, V>::const_iterator const_iterator;

  concurrent_map() : map_(), mutex_() {}
  explicit concurrent_map(int32_t n) : map_(n), mutex_() {}

  void insert(const value_type& value) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    map_.insert(value);
  }

  /**
   * Return the mapped object for this key. Be careful to not use the return reference
   * to do assignment. I think it won't be thread safe
   */
  mapped_type& at(const key_type& key) {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    iterator where = map_.find(key);
    if (where == end()) {
      std::runtime_error("Key not found");
    }
    return where->second;
  }

  mapped_type& operator[](const key_type& key) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    iterator where = map_.find(key);
    if (where == end()) {
      return map_[key];
    }
    return where->second;
  }

  /**
   * Atomically finds the entry and removes it from the map, returning
   * the previously associated value.
   */
  mapped_type find_and_erase(const K& key) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    auto search = map_.find(key);
    // It's an error if it's not there.
    CHECK(search != end());
    auto val = std::move(search->second);
    map_.erase(key);
    return val;
  }

  void erase(const K& key) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    map_.erase(key);
  }

  iterator begin() { return map_.begin(); }

  const_iterator begin() const { return map_.begin(); }

  const_iterator cbegin() const { return map_.begin(); }

  iterator end() { return map_.end(); }

  const_iterator end() const { return map_.end(); }

  const_iterator cend() const { return map_.end(); }

  iterator find(const K& key) {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return map_.find(key);
  }

  // TODO: find(), at() returning const_iterator

  bool empty() const {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return map_.empty();
  }

 private:
  std::shared_timed_mutex mutex_;
  std::unordered_map<K, V> map_;
};
} /* namespace hbase */
