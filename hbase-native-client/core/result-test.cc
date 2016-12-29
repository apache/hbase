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

#include "core/result.h"

#include <limits>
#include <memory>
#include <vector>
#include <string>
#include <gtest/gtest.h>

#include "core/cell.h"
using namespace hbase;

void PopulateCells(std::vector<std::shared_ptr<Cell> > &cells) {
  // Populate some Results
  // We assume that for a single Cell, the corresponding row, families and
  // qualifiers are present.
  // We have also considered different versions in the test for the same row.
  std::string row = "row";
  for (int i = 0; i < 10; i++) {
    std::string family = "family-" + std::to_string(i);
    std::string column = "column-" + std::to_string(i);
    std::string value = "value-" + std::to_string(i);

    switch (i) {
      case 5: {
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482113040506, "value-5", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482111803856, "value-X", CellType::PUT));
        break;
      }
      case 8: {
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482113040506, "value-8", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482111803856, "value-X", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482110969958, "value-Y", CellType::PUT));
        break;
      }
      case 9: {
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482113040506, "value-9", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482111803856, "value-X", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482110969958, "value-Y", CellType::PUT));
        cells.push_back(std::make_shared<Cell>(
            row, family, column, 1482110876075, "value-Z", CellType::PUT));
        break;
      }
      default: {
        cells.push_back(std::make_shared<Cell>(row, family, column,
                                               std::numeric_limits<long>::max(),
                                               value, CellType::PUT));
      }
    }
  }
  return;
}

TEST(Result, EmptyResult) {
  std::vector<std::shared_ptr<Cell> > cells;
  Result result(cells, true, false, false);
  EXPECT_EQ(true, result.IsEmpty());
  EXPECT_EQ(0, result.Size());
}

TEST(Result, FilledResult) {
  std::vector<std::shared_ptr<Cell> > cells;
  PopulateCells(cells);

  Result result(cells, true, false, false);

  EXPECT_EQ(false, result.IsEmpty());
  EXPECT_EQ(16, result.Size());

  // Get Latest Cell for the given family and qualifier.
  auto latest_cell(result.ColumnLatestCell("family", "column"));
  // Nothing of the above family/qualifier combo is present so it should be
  // nullptr
  ASSERT_FALSE(latest_cell.get());

  // Try to get the latest cell for the given family and qualifier.
  latest_cell = result.ColumnLatestCell("family-4", "column-4");
  // Now shouldn't be a nullptr
  ASSERT_TRUE(latest_cell.get());
  // And Value must match too
  EXPECT_EQ("value-4", latest_cell->Value());

  // Value will be nullptr as no such family and qualifier is present
  ASSERT_FALSE(result.Value("family-4", "qualifier"));
  // Value will be present as family and qualifier is present
  ASSERT_TRUE(result.Value("family-4", "column-4"));
  // Value should be present and match.
  EXPECT_EQ(latest_cell->Value(),
            (*result.ColumnLatestCell("family-4", "column-4")).Value());
  EXPECT_EQ("value-5",
            (*result.ColumnLatestCell("family-5", "column-5")).Value());
  EXPECT_EQ("value-8",
            (*result.ColumnLatestCell("family-8", "column-8")).Value());
  EXPECT_EQ("value-7", *result.Value("family-7", "column-7"));

  // Get cells for the given family and qualifier
  auto column_cells = result.ColumnCells("family", "column");
  // Size should be 0
  EXPECT_EQ(0, column_cells.size());

  // Size shouldn't be 0 and Row() and Value() must match
  column_cells = result.ColumnCells("family-0", "column-0");
  EXPECT_EQ(1, column_cells.size());
  EXPECT_EQ("row", column_cells[0]->Row());
  EXPECT_EQ("row", result.Row());

  // Size shouldn't be 0 and Row() and Value() must match
  column_cells = result.ColumnCells("family-5", "column-5");
  EXPECT_EQ(2, column_cells.size());
  EXPECT_EQ("row", column_cells[0]->Row());
  EXPECT_EQ("row", column_cells[1]->Row());
  EXPECT_EQ("value-5", column_cells[0]->Value());
  EXPECT_EQ("value-X", column_cells[1]->Value());
  EXPECT_EQ("row", result.Row());

  // Size shouldn't be 0 and Row() and Value() must match
  column_cells = result.ColumnCells("family-8", "column-8");
  EXPECT_EQ(3, column_cells.size());
  EXPECT_EQ("row", column_cells[0]->Row());
  EXPECT_EQ("row", column_cells[1]->Row());
  EXPECT_EQ("row", column_cells[2]->Row());
  EXPECT_EQ("value-8", column_cells[0]->Value());
  EXPECT_EQ("value-X", column_cells[1]->Value());
  EXPECT_EQ("value-Y", column_cells[2]->Value());
  EXPECT_EQ("row", result.Row());

  // Size shouldn't be 0 and Row() and Value() must match
  column_cells = result.ColumnCells("family-9", "column-9");
  EXPECT_EQ(4, column_cells.size());
  EXPECT_EQ("row", column_cells[0]->Row());
  EXPECT_EQ("row", column_cells[1]->Row());
  EXPECT_EQ("row", column_cells[2]->Row());
  EXPECT_EQ("row", column_cells[3]->Row());
  EXPECT_EQ("value-9", column_cells[0]->Value());
  EXPECT_EQ("value-X", column_cells[1]->Value());
  EXPECT_EQ("value-Y", column_cells[2]->Value());
  EXPECT_EQ("value-Z", column_cells[3]->Value());
  EXPECT_EQ("row", result.Row());

  // Test all the Cell values
  const auto &result_cells = result.Cells();
  int i = 0, j = 0;
  for (const auto &cell : result_cells) {
    std::string row = "row";
    std::string family = "family-" + std::to_string(i);
    std::string column = "column-" + std::to_string(i);
    std::string value = "value-" + std::to_string(i);
    switch (j) {
      case 6:
      case 10:
      case 13: {
        EXPECT_EQ("value-X", cell->Value());
        ++j;
        continue;
      }
      case 11:
      case 14: {
        EXPECT_EQ("value-Y", cell->Value());
        ++j;
        continue;
      }
      case 15: {
        EXPECT_EQ("value-Z", cell->Value());
        ++j;
        continue;
      }
    }
    EXPECT_EQ(row, cell->Row());
    EXPECT_EQ(family, cell->Family());
    EXPECT_EQ(column, cell->Qualifier());
    EXPECT_EQ(value, cell->Value());
    ++i;
    ++j;
  }

  auto result_map_tmp = result.Map();
  result_map_tmp["testf"]["testq"][1] = "value";
  EXPECT_EQ(11, result_map_tmp.size());

  auto result_map = result.Map();
  EXPECT_EQ(10, result_map.size());

  i = 0;
  for (auto family_map : result_map) {
    std::string family = "family-" + std::to_string(i);
    std::string qualifier = "column-" + std::to_string(i);
    std::string value = "value-" + std::to_string(i);
    EXPECT_EQ(family, family_map.first);
    for (auto qualifier_map : family_map.second) {
      EXPECT_EQ(qualifier, qualifier_map.first);
      j = 0;
      for (auto version_map : qualifier_map.second) {
        switch (i) {
          case 5: {
            if (1 == j) {
              EXPECT_EQ(1482111803856, version_map.first);
              EXPECT_EQ("value-X", version_map.second);
            } else if (0 == j) {
              EXPECT_EQ(1482113040506, version_map.first);
              EXPECT_EQ("value-5", version_map.second);
            }
            break;
          }
          case 8: {
            if (2 == j) {
              EXPECT_EQ(1482110969958, version_map.first);
              EXPECT_EQ("value-Y", version_map.second);
            } else if (1 == j) {
              EXPECT_EQ(1482111803856, version_map.first);
              EXPECT_EQ("value-X", version_map.second);
            } else if (0 == j) {
              EXPECT_EQ(1482113040506, version_map.first);
              EXPECT_EQ("value-8", version_map.second);
            }
            break;
          }
          case 9: {
            if (3 == j) {
              EXPECT_EQ(1482110876075, version_map.first);
              EXPECT_EQ("value-Z", version_map.second);
            } else if (2 == j) {
              EXPECT_EQ(1482110969958, version_map.first);
              EXPECT_EQ("value-Y", version_map.second);
            } else if (1 == j) {
              EXPECT_EQ(1482111803856, version_map.first);
              EXPECT_EQ("value-X", version_map.second);
            } else if (0 == j) {
              EXPECT_EQ(1482113040506, version_map.first);
              EXPECT_EQ("value-9", version_map.second);
            }
            break;
          }
          default: {
            EXPECT_EQ(std::numeric_limits<long>::max(), version_map.first);
            EXPECT_EQ(value, version_map.second);
          }
        }
        ++j;
      }
    }
    ++i;
  }

  auto family_map = result.FamilyMap("family-0");
  EXPECT_EQ(1, family_map.size());
  i = 0;
  for (auto qual_val_map : family_map) {
    EXPECT_EQ("column-0", qual_val_map.first);
    EXPECT_EQ("value-0", qual_val_map.second);
  }

  family_map = result.FamilyMap("family-1");
  EXPECT_EQ(1, family_map.size());
  i = 0;
  for (auto qual_val_map : family_map) {
    EXPECT_EQ("column-1", qual_val_map.first);
    EXPECT_EQ("value-1", qual_val_map.second);
  }

  family_map = result.FamilyMap("family-5");
  EXPECT_EQ(1, family_map.size());
  i = 0;
  for (auto qual_val_map : family_map) {
    EXPECT_EQ("column-5", qual_val_map.first);
    EXPECT_EQ("value-5", qual_val_map.second);
  }

  family_map = result.FamilyMap("family-9");
  EXPECT_EQ(1, family_map.size());
  i = 0;
  for (auto qual_val_map : family_map) {
    EXPECT_EQ("column-9", qual_val_map.first);
    EXPECT_EQ("value-9", qual_val_map.second);
  }
}
