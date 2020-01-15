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

; (function ($, window, document, undefined) {
  'use strict';
  var defaults = {
    page: 1,
    pageSize: 10,
    total: 0,
    noData: false,// Whether to show pagination without data, default false does not show, true shows the first page
    showPN: true,// Whether to show up and down page
    prevPage: "prev",//Page up button text
    nextPage: "next",//Page down button text
    fastForward: 0,//Fast forward fast rewind, default 0 page
    backFun: function (page) {}//Click the page button callback function to return the current page number
  };
  function Plugin(element, options) {
    this.element = $(element);
    this.settings = $.extend({}, defaults, options);
    this.pageNum = 1,
    this.pageList = [],
    this.pageTatol = 0;
    this.init();
  }
  $.extend(Plugin.prototype, {
    init: function () {
      this.element.empty();
      this.viewHtml();
    },
    creatHtml: function (i) {
      i == this.settings.page ? this.pageList.push('<li class="active"><a  href="javascript:void(0)" data-page=' + i + ">" + i + "</a></li>")
        : this.pageList.push("<li><a href='javascript:void(0)'  data-page=" + i + ">" + i + "</a></li>");
    },
    viewHtml: function () {
      var settings = this.settings;
      var pageTatol = 0;
      var pageArr = [];
      if (settings.total > 0) {
        pageTatol = Math.ceil(settings.total / settings.pageSize);
      } else {
        if (settings.noData) {
          pageTatol = 1;
          settings.page = 1;
          settings.total = 0;
        } else {
          return;
        }
      }
      this.pageTatol = pageTatol;
      this.pageNum = settings.page;
      pageArr.push('<ul class="pagination">');
      this.pageList = [];
      if (settings.showPN) {
        settings.page == 1 ? this.pageList.push('<li class="disabled" ><a href="javascript:void(0)" data-page="prev">' + settings.prevPage + '</a></li>')
          : this.pageList.push('<li><a href="javascript:void(0)" data-page="prev">' + settings.prevPage + '</a></li>');

      }
      if (pageTatol <= 6) {
        for (var i = 1; i < pageTatol + 1; i++) {
          this.creatHtml(i);
        }
      } else {
        if (settings.page < 5) {
          for (var i = 1; i <= 5; i++) {
            this.creatHtml(i);
          }

          this.pageList.push('<li><a href="javascript:void(0)" data-page="after" class="spage-after">...</a></li><li><a href="javascript:void(0)" data-page=' + pageTatol + ">" + pageTatol + '</a></li>');

        } else if (settings.page > pageTatol - 4) {
          this.pageList.push('<li><a href="javascript:void(0)" data-page="1">1</a></li><li><a href="javascript:void(0)" data-page="before" class="spage-before">...</a></li>');
          for (var i = pageTatol - 4; i <= pageTatol; i++) {
            this.creatHtml(i);
          }
        } else {
          this.pageList.push('<li><a href="javascript:void(0)" data-page="1">1</a></li><li><a  href="javascript:void(0)" data-page="before" class="spage-before">...</a></li>');
          for (var i = settings.page - 2; i <= Number(settings.page) + 2; i++) {
            this.creatHtml(i);
          }
          this.pageList.push('<li><a href="javascript:void(0)" data-page="after" class="spage-after">...</a></li><li><a href="javascript:void(0)" data-page=' + pageTatol + '">' + pageTatol + '</a></li>');
        }
      }
      if (settings.showPN) {
        settings.page == pageTatol ? this.pageList.push('<li class="disabled"><a href="javascript:void(0)"  data-page="next">' + settings.nextPage + "</a></li>")
          : this.pageList.push('<li><a data-page="next" href="javascript:void(0)">' + settings.nextPage + '</a></li>');
      }
      pageArr.push(this.pageList.join(''));
      pageArr.push('</ul>');

      this.element.html(pageArr.join(''));
      this.clickBtn();
    },
    clickBtn: function () {
      var that = this;
      var settings = this.settings;
      var ele = this.element;
      var pageTatol = this.pageTatol;
      this.element.off("click", "a");
      this.element.on("click", "a", function () {
        var pageNum = $(this).data("page");
        switch (pageNum) {
          case "prev":
            settings.page = settings.page - 1 >= 1 ? settings.page - 1 : 1;
            pageNum = settings.page;
            break;
          case "next":
            settings.page = Number(settings.page) + 1 <= pageTatol ? Number(settings.page) + 1 : pageTatol;
            pageNum = settings.page;
            break;
          case "before":
            settings.page = settings.page - settings.fastForward >= 1 ? settings.page - settings.fastForward : 1;
            pageNum = settings.page;
            break;
          case "after":
            settings.page = Number(settings.page) + Number(settings.fastForward) <= pageTatol ? Number(settings.page) + Number(settings.fastForward) : pageTatol;
            pageNum = settings.page;
            break;
          case "go":
            if (/^[0-9]*$/.test(p) && p >= 1 && p <= pageTatol) {
              settings.page = p;
            } else {
              return;
            }
            break;
          default:
            settings.page = pageNum;
        }

        that.pageNum = settings.page;
        that.viewHtml();
        settings.backFun(pageNum);
      });

      if(settings.fastForward > 0){
        ele.find(".spage-after").hover(function(){
          $(this).html("&nbsp;&raquo;");
        },function(){
          $(this).html("...");
        });
        ele.find(".spage-before").hover(function(){
          $(this).html("&nbsp;&laquo;");
        },function(){
          $(this).html("...");
        });
      }
    }
  });
  $.fn.sPage = function (options) {
    return this.each(function(){
      new Plugin(this, options);
    });
  }
})(jQuery, window, document);
