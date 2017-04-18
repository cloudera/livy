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

function loadBatchesTable(id, sessions) {
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

$(document).ready(function () {
    $.getJSON(location.origin + "/sessions", function(response) {
        if (response && response.total > 0) {
            $("#all-sessions").append('<div id="interactive-sessions"></div>');
            $("#interactive-sessions").load("/static/sessions-table.html", function() {
                loadSessionsTable(response.sessions);
            });
        }
    });
    $.getJSON(location.origin + "/batches", function(response) {
        if (response && response.total > 0) {
            $("#all-sessions").append('<div id="batches"></div>');
            $("#batches").load("/static/batches-table.html", function() {
                loadBatchesTable(response.sessions);
            });
        }
    });
});