#pragma once

#include "if/HBase.pb.h"
#include <folly/Conv.h>
#include <folly/String.h>

namespace hbase {
namespace pb {

template <class String> void parseTo(String in, ServerName &out) {
  // TODO see about getting rsplit into folly.
  std::string s = folly::to<std::string>(in);

  auto delim = s.rfind(":");
  DCHECK(delim != std::string::npos);
  out.set_host_name(s.substr(0, delim));
  // Now keep everything after the : (delim + 1) to the end.
  out.set_port(folly::to<int>(s.substr(delim + 1)));
}
}
}
