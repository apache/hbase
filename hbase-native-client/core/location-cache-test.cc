#include <gtest/gtest.h>
#include <folly/Memory.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include "location-cache.h"
using namespace hbase;

TEST(LocationCacheTest, TestGetMetaNodeContents) {
  // TODO(elliott): need to make a test utility for this.
  LocationCache cache{"localhost:2181", wangle::getCPUExecutor()};
  auto result = cache.LocateMeta();
  result.wait();
  ASSERT_FALSE(result.hasException());
}
