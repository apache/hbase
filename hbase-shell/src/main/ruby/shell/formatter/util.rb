#
# Copyright The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
module Shell
  module Formatter
    module Util
      # Width of ISO8601 dates (with millisecond precision)
      ISO8601_WIDTH = 24

      def self.to_iso_8601(millis)
        java.time.Instant.ofEpochMilli(millis).toString
      end

      ##
      # right-pad with spaces or truncate with ellipses to match passed width
      #
      # Note that this function will not handle special characters such as newlines, tabs, and
      # non-printing characters.
      #
      # @param [String] text to truncate or pad
      # @param [Integer] width to match
      # @param [Boolean] pad should this function pad text that is too short?
      # @param [Boolean] truncate should this function truncate text that is too long?
      # @return [String] padded/truncated cell content
      def self.set_text_width(text, width, pad: true, truncate: true)
        # TODO: Use unicode ellipses if the terminal emulator supports it
        num_too_short = width - text.length
        if num_too_short < 0
          # text is too long, so truncate
          return text unless truncate

          text[0, [width - 3, 0].max] + '.' * [3, width].min
        else
          # text is requested width or too short, so right-pad with zero or more spaces
          return text unless pad

          text + ' ' * num_too_short
        end
      end
    end
  end
end
