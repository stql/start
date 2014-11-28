// Place all the behaviors and hooks related to the matching controller here.
// All this logic will automatically be available in application.js.

$('.queries.index').ready(function(){
  $('#query_result').dataTable({
    "order": [[0, 'desc']],
    "aoColumns": [
      null,
      {"bSortable": false},
      {"bSortable": false},
      null,
      {"bSortable": false},
      {"bSortable": false}
    ],
    "dom": 'tip'
  });
});
