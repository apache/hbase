/*
 * Copyright The Apache Software Foundation
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

$(document).ready(function()
  {
    $.tablesorter.addParser({
      id: 'filesize',
      is: function(s) {
        return s.match(new RegExp( /([\.0-9]+)\ (B|KB|MB|GB|TB)/ ));
      },
      format: function(s) {
        var suf = s.match(new RegExp( /(KB|B|GB|MB|TB)$/ ))[1];
        var num = parseFloat(s.match( new RegExp( /([\.0-9]+)\ (B|KB|MB|GB|TB)/ ))[0]);
        switch(suf) {
          case 'B':
            return num;
          case 'KB':
            return num * 1024;
          case 'MB':
            return num * 1024 * 1024;
          case 'GB':
            return num * 1024 * 1024 * 1024;
          case 'TB':
            return num * 1024 * 1024 * 1024 * 1024;
        }
      },
      type: 'numeric'
    });
    $.tablesorter.addParser(
      {
        id: "separator",
        is: function (s) {
          return /^[0-9]?[0-9,]*$/.test(s);
        },
        format: function (s) {
          return $.tablesorter.formatFloat( s.replace(/,/g,'') );
        },
        type: "numeric"
      });

    $("#baseStatsTable").tablesorter({
      headers: {
        '.cls_emptyMin': {empty: 'emptyMin'},
        '.cls_emptyMax': {empty: 'emptyMax'}
      }
    });
    $("#requestStatsTable").tablesorter({
      headers: {
        '.cls_separator': {sorter: 'separator'}
      }
    });
    $("#storeStatsTable").tablesorter({
      headers: {
        '.cls_separator': {sorter: 'separator'},
        '.cls_filesize': {sorter: 'filesize'}
      }
    });
    $("#compactionStatsTable").tablesorter({
      headers: {
        '.cls_separator': {sorter: 'separator'}
      }
    });
    $("#memstoreStatsTable").tablesorter({
      headers: {
        '.cls_filesize': {sorter: 'filesize'}
      }
    });
  }
);
