/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

function tdWrap(str) {
  return "<td>" + str + "</td>";
}

function loadSessionsTable(sessions) {
  $.each(sessions, function(index, session) {
    $("#interactive-sessions .sessions-table-body").append(
      "<tr>" +
        tdWrap(session.id) +
        tdWrap(session.appId) +
        tdWrap(session.owner) +
        tdWrap(session.proxyUser) +
        tdWrap(session.kind) +
        tdWrap(session.state) +
       "</tr>"
    );
  });
}

function loadBatchesTable(sessions) {
  $.each(sessions, function(index, session) {
    $("#batches .sessions-table-body").append(
      "<tr>" +
        tdWrap(session.id) +
        tdWrap(session.appId) +
        tdWrap(session.state) +
       "</tr>"
    );
  });
}

var numSessions = 0;
var numBatches = 0;

$(document).ready(function () {
  $.extend( $.fn.dataTable.defaults, {
    stateSave: true,
  });

  var sessionsReq = $.getJSON(location.origin + "/sessions", function(response) {
    if (response && response.total > 0) {
      $("#interactive-sessions").load("/static/sessions-table.html .sessions-template", function() {
        loadSessionsTable(response.sessions);
        $("#interactive-sessions-table").DataTable();
        $('#interactive-sessions [data-toggle="tooltip"]').tooltip();
      });
    }
    numSessions = response.total;
  });

  var batchesReq = $.getJSON(location.origin + "/batches", function(response) {
    if (response && response.total > 0) {
      $("#batches").load("/static/batches-table.html .sessions-template", function() {
        loadBatchesTable(response.sessions);
        $("#batches-table").DataTable();
        $('#batches [data-toggle="tooltip"]').tooltip();
      });
    }
    numBatches = response.total;
  });

  $.when(sessionsReq, batchesReq).done(function () {
    if (numSessions + numBatches == 0) {
      $("#all-sessions").append('<h4>No Sessions or Batches have been created yet.</h4>');
    }
  });
});