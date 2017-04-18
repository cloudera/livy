$(function() {
    $("#sessions").load("/static/sessions.html");
});

function tdWrap(str) {
    return "<td>" + str + "</td>";
}

$(document).ready(function () {
    $.getJSON(location.origin + "/sessions", function(response) {
        if (response && response.total > 0) {
            var sessions = response.sessions;
            $.each(sessions, function(index, session) {
                $("#sessions-table-body").append(
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
            return;
        }
    });
});