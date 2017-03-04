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

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "if/Comparator.pb.h"
#include "if/Filter.pb.h"
#include "if/HBase.pb.h"

using google::protobuf::Message;

namespace hbase {

/**
 * In C++ Client, Filter is a thin wrapper for calling filters defined as a Java class. The actual
 * filtering logic is not implemented here, but this class provides a mechanism to call
 * pre-existing Filter classes (like KeyOnlyFilter, SingleColumnValueFilter, etc) with your Get or
 * Scan RPCs. This class can also be used to call custom Filters defined as a Java class, or
 * pre-existing Filters not defined below. Some of the interfaces depends on protobuf classes
 * defined in HBase.proto, Filter.proto and Comparator.proto.
 *
 * Consult the Java class docs for learning about the various filters and how they work (and filter
 * arguments).
 *
 * Pre-existing Filters can be used like this:
 *
 * Get get(row);
 * get.SetFilter(FilterFactory::ColumnPrefixFilter("foo_"));
 *
 * Custom filters can be invoked like this:
 * Get get(row);
 * std::string filter_java_class_name = "foo.bar.baz";
 * auto filter_data = std::make_unique<pb::MyFilter>();
 * filter_data->set_foo(foo);
 * get.SetFilter(std::make_unique<Filter>(filter_java_class_name, filter_data));
 *
 */
class Filter {
 public:
  Filter(std::string java_class_name, std::unique_ptr<Message> data)
      : java_class_name_(java_class_name), data_(std::move(data)) {}
  virtual ~Filter() {}

  const std::string java_class_name() const { return java_class_name_; }

  const Message& data() const { return *data_; }
  /**
   * Serialize the filter data to the given buffer. Does protobuf encoding by default.
   * Can be overriden if Filter does not use protobuf.
   */
  virtual void Serialize(std::string* buf) const {
    if (data_ != nullptr) {
      data_->SerializeToString(buf);
    }
  }

  /** Internal method */
  static std::unique_ptr<pb::Filter> ToProto(const Filter& filter) {
    auto pb_filter = std::make_unique<pb::Filter>();
    pb_filter->set_name(filter.java_class_name());
    filter.Serialize(pb_filter->mutable_serialized_filter());
    return std::move(pb_filter);
  }

 private:
  std::unique_ptr<Message> data_;
  std::string java_class_name_;
};

/**
 * Comparator for filters. See ByteArrayComparable documentation in Java.
 */
class Comparator {
 public:
  Comparator(std::string java_class_name, std::unique_ptr<Message> data)
      : java_class_name_(java_class_name), data_(std::move(data)) {}
  virtual ~Comparator() {}

  const std::string java_class_name() const { return java_class_name_; }

  /**
   * Serialize the Comparator data to the given buffer. Does protobuf encoding by default.
   * Can be overriden if Comparator does not use protobuf.
   */
  virtual void Serialize(std::string* buf) const {
    if (data_ != nullptr) {
      data_->SerializeToString(buf);
    }
  }

  /** Internal method */
  static std::unique_ptr<pb::Comparator> ToProto(const Comparator& comparator) {
    auto pb_comparator = std::make_unique<pb::Comparator>();
    pb_comparator->set_name(comparator.java_class_name());
    comparator.Serialize(pb_comparator->mutable_serialized_comparator());
    return std::move(pb_comparator);
  }

 private:
  std::unique_ptr<Message> data_;
  std::string java_class_name_;
};

/**
 * Used in row range filters
 */
struct RowRange {
  std::string start_row;
  bool start_row_inclusive;
  std::string stop_row;
  bool stop_row_inclusive;
};

/**
 * Factory for creating pre-defined filters.
 */
class FilterFactory {
 public:
  static std::unique_ptr<Filter> ColumnCountGetFilter(uint32_t limit) noexcept {
    auto data = std::make_unique<pb::ColumnCountGetFilter>();
    data->set_limit(limit);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ColumnCountGetFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> ColumnPaginationFilter(uint32_t limit, uint32_t offset) noexcept {
    auto data = std::make_unique<pb::ColumnPaginationFilter>();
    data->set_limit(limit);
    data->set_offset(offset);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ColumnPaginationFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> ColumnPaginationFilter(uint32_t limit,
                                                        const std::string& column_offset) noexcept {
    auto data = std::make_unique<pb::ColumnPaginationFilter>();
    data->set_limit(limit);
    data->set_column_offset(column_offset);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ColumnPaginationFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> ColumnPrefixFilter(const std::string& prefix) noexcept {
    auto data = std::make_unique<pb::ColumnPrefixFilter>();
    data->set_prefix(prefix);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ColumnPrefixFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> ColumnRangeFilter(const std::string& min_column,
                                                   bool min_column_inclusive,
                                                   const std::string& max_column,
                                                   bool max_column_inclusive) noexcept {
    auto data = std::make_unique<pb::ColumnRangeFilter>();
    data->set_min_column(min_column);
    data->set_min_column_inclusive(min_column_inclusive);
    data->set_max_column(max_column);
    data->set_max_column_inclusive(max_column_inclusive);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ColumnRangeFilter",
                                    std::move(data));
  }

  static std::unique_ptr<pb::CompareFilter> CompareFilter(pb::CompareType compare_op,
                                                          const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::CompareFilter>();
    data->set_compare_op(compare_op);
    data->set_allocated_comparator(Comparator::ToProto(comparator).release());
    return std::move(data);
  }

  /**
    * Build a dependent column filter with value checking
    * dependent column varies will be compared using the supplied
    * compareOp and comparator, for usage of which
    * refer to {@link CompareFilter}
    *
    * @param family dependent column family
    * @param qualifier dependent column qualifier
    * @param drop_dependent_column whether the column should be discarded after
    * @param compare_op comparison op
    * @param comparator comparator
    */
  static std::unique_ptr<Filter> DependentColumnFilter(const std::string& family,
                                                       const std::string& qualifier,
                                                       bool drop_dependent_column,
                                                       pb::CompareType compare_op,
                                                       const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::DependentColumnFilter>();
    data->set_column_family(family);
    data->set_column_qualifier(qualifier);
    data->set_drop_dependent_column(drop_dependent_column);
    data->set_allocated_compare_filter(CompareFilter(compare_op, comparator).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.DependentColumnFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> FamilyFilter(pb::CompareType compare_op,
                                              const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::FamilyFilter>();
    data->set_allocated_compare_filter(CompareFilter(compare_op, comparator).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.FamilyFilter", std::move(data));
  }

  static std::unique_ptr<Filter> FilterAllFilter() noexcept {
    auto data = std::make_unique<pb::FilterAllFilter>();
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.FilterAllFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> FilterList(
      pb::FilterList_Operator op, const std::vector<std::unique_ptr<Filter>>& filters) noexcept {
    auto data = std::make_unique<pb::FilterList>();
    data->set_operator_(op);
    for (auto const& f : filters) {
      data->mutable_filters()->AddAllocated(Filter::ToProto(*f).release());
    }
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.FilterList", std::move(data));
  }

  static std::unique_ptr<Filter> FirstKeyOnlyFilter() noexcept {
    auto data = std::make_unique<pb::FirstKeyOnlyFilter>();
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> FirstKeyValueMatchingQualifiersFilter(
      const std::set<std::string>& qualifiers) noexcept {
    auto data = std::make_unique<pb::FirstKeyValueMatchingQualifiersFilter>();
    for (auto q : qualifiers) {
      data->add_qualifiers(q);
    }
    return std::make_unique<Filter>(
        "org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter", std::move(data));
  }

  static std::unique_ptr<Filter> FuzzyRowFilter(
      const std::vector<std::pair<std::string, std::string>>& fuzzy_keys_data) noexcept {
    auto data = std::make_unique<pb::FuzzyRowFilter>();
    for (auto q : fuzzy_keys_data) {
      auto p = data->add_fuzzy_keys_data();
      p->set_first(q.first);
      p->set_second(q.second);
    }
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.FuzzyRowFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> InclusiveStopFilter(const std::string& stop_row_key) noexcept {
    auto data = std::make_unique<pb::InclusiveStopFilter>();
    data->set_stop_row_key(stop_row_key);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.InclusiveStopFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> KeyOnlyFilter(bool len_as_val) noexcept {
    auto data = std::make_unique<pb::KeyOnlyFilter>();
    data->set_len_as_val(len_as_val);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.KeyOnlyFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> MultipleColumnPrefixFilter(
      const std::vector<std::string>& sorted_prefixes) noexcept {
    auto data = std::make_unique<pb::MultipleColumnPrefixFilter>();
    for (auto p : sorted_prefixes) {
      data->add_sorted_prefixes(p);
    }
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> MultiRowRangeFilter(
      const std::vector<RowRange>& row_ranges) noexcept {
    auto data = std::make_unique<pb::MultiRowRangeFilter>();
    for (auto r : row_ranges) {
      auto range = data->add_row_range_list();
      range->set_start_row(r.start_row);
      range->set_start_row_inclusive(r.start_row_inclusive);
      range->set_stop_row(r.stop_row);
      range->set_stop_row_inclusive(r.stop_row_inclusive);
    }
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.MultiRowRangeFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> PageFilter(uint64_t page_size) noexcept {
    auto data = std::make_unique<pb::PageFilter>();
    data->set_page_size(page_size);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.PageFilter", std::move(data));
  }

  static std::unique_ptr<Filter> PrefixFilter(const std::string& prefix) noexcept {
    auto data = std::make_unique<pb::PrefixFilter>();
    data->set_prefix(prefix);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.PrefixFilter", std::move(data));
  }

  static std::unique_ptr<Filter> QualifierFilter(pb::CompareType compare_op,
                                                 const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::QualifierFilter>();
    data->set_allocated_compare_filter(CompareFilter(compare_op, comparator).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.QualifierFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> RandomRowFilter(float chance) noexcept {
    auto data = std::make_unique<pb::RandomRowFilter>();
    data->set_chance(chance);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.RandomRowFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> RowFilter(pb::CompareType compare_op,
                                           const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::RowFilter>();
    data->set_allocated_compare_filter(CompareFilter(compare_op, comparator).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.RowFilter", std::move(data));
  }

  static std::unique_ptr<Filter> SingleColumnValueExcludeFilter(
      const std::string& family, const std::string& qualifier, bool filter_if_missing,
      bool latest_version_only, pb::CompareType compare_op, const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::SingleColumnValueExcludeFilter>();
    auto f = SingleColumnValueFilterProto(family, qualifier, filter_if_missing, latest_version_only,
                                          compare_op, comparator);
    data->set_allocated_single_column_value_filter(f.release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter",
                                    std::move(data));
  }

  static std::unique_ptr<pb::SingleColumnValueFilter> SingleColumnValueFilterProto(
      const std::string& family, const std::string& qualifier, bool filter_if_missing,
      bool latest_version_only, pb::CompareType compare_op, const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::SingleColumnValueFilter>();
    data->set_column_family(family);
    data->set_column_qualifier(qualifier);
    data->set_compare_op(compare_op);
    data->set_filter_if_missing(filter_if_missing);
    data->set_latest_version_only(latest_version_only);
    data->set_allocated_comparator(Comparator::ToProto(comparator).release());
    return data;
  }

  static std::unique_ptr<Filter> SingleColumnValueFilter(
      const std::string& family, const std::string& qualifier, bool filter_if_missing,
      bool latest_version_only, pb::CompareType compare_op, const Comparator& comparator) noexcept {
    auto data = SingleColumnValueFilterProto(family, qualifier, filter_if_missing,
                                             latest_version_only, compare_op, comparator);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.SingleColumnValueFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> SkipFilter(const Filter& filter) noexcept {
    auto data = std::make_unique<pb::SkipFilter>();
    data->set_allocated_filter(Filter::ToProto(filter).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.SkipFilter", std::move(data));
  }

  static std::unique_ptr<Filter> TimestampsFilter(std::vector<uint64_t> timestamps,
                                                  bool can_hint) noexcept {
    auto data = std::make_unique<pb::TimestampsFilter>();
    for (auto t : timestamps) {
      data->add_timestamps(t);
    }
    data->set_can_hint(can_hint);
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.TimestampsFilter",
                                    std::move(data));
  }

  static std::unique_ptr<Filter> ValueFilter(pb::CompareType compare_op,
                                             const Comparator& comparator) noexcept {
    auto data = std::make_unique<pb::ValueFilter>();
    data->set_allocated_compare_filter(CompareFilter(compare_op, comparator).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.ValueFilter", std::move(data));
  }

  static std::unique_ptr<Filter> WhileMatchFilter(const Filter& filter) noexcept {
    auto data = std::make_unique<pb::WhileMatchFilter>();
    data->set_allocated_filter(Filter::ToProto(filter).release());
    return std::make_unique<Filter>("org.apache.hadoop.hbase.filter.WhileMatchFilter",
                                    std::move(data));
  }
};

/**
 * Factory for creating pre-defined Comparators.
 */
class ComparatorFactory {
 public:
  static std::unique_ptr<pb::ByteArrayComparable> ByteArrayComparable(
      const std::string& value) noexcept {
    auto data = std::make_unique<pb::ByteArrayComparable>();
    data->set_value(value);
    return std::move(data);
  }

  static std::unique_ptr<Comparator> BinaryComparator(const std::string& value) noexcept {
    auto data = std::make_unique<pb::BinaryComparator>();
    data->set_allocated_comparable(ByteArrayComparable(value).release());
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.BinaryComparator",
                                        std::move(data));
  }

  static std::unique_ptr<Comparator> LongComparator(const std::string& value) noexcept {
    // TODO: this should take a uint64_t argument, not a byte array.
    auto data = std::make_unique<pb::LongComparator>();
    data->set_allocated_comparable(ByteArrayComparable(value).release());
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.LongComparator",
                                        std::move(data));
  }

  static std::unique_ptr<Comparator> BinaryPrefixComparator(const std::string& value) noexcept {
    auto data = std::make_unique<pb::BinaryPrefixComparator>();
    data->set_allocated_comparable(ByteArrayComparable(value).release());
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.BinaryPrefixComparator",
                                        std::move(data));
  }

  static std::unique_ptr<Comparator> BitComparator(const std::string& value,
                                                   pb::BitComparator_BitwiseOp bit_op) noexcept {
    auto data = std::make_unique<pb::BitComparator>();
    data->set_allocated_comparable(ByteArrayComparable(value).release());
    data->set_bitwise_op(bit_op);
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.BitComparator",
                                        std::move(data));
  }

  static std::unique_ptr<Comparator> NullComparator() noexcept {
    auto data = std::make_unique<pb::NullComparator>();
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.NullComparator",
                                        std::move(data));
  }

  /**
   * @param pattern a valid regular expression
   * @param pattern_flags java.util.regex.Pattern flags
   * @param charset the charset name
   * @param engine engine implementation type, either JAVA or JONI
   */
  static std::unique_ptr<Comparator> RegexStringComparator(
      const std::string& pattern, int32_t pattern_flags, const std::string& charset = "UTF-8",
      const std::string& engine = "JAVA") noexcept {
    auto data = std::make_unique<pb::RegexStringComparator>();
    data->set_pattern(pattern);
    data->set_pattern_flags(pattern_flags);
    data->set_charset(charset);
    data->set_engine(engine);
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.RegexStringComparator",
                                        std::move(data));
  }

  static std::unique_ptr<Comparator> SubstringComparator(const std::string& substr) noexcept {
    auto data = std::make_unique<pb::SubstringComparator>();
    data->set_substr(substr);
    return std::make_unique<Comparator>("org.apache.hadoop.hbase.filter.SubstringComparator",
                                        std::move(data));
  }
};
}  // namespace hbase
