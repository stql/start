// Place all the behaviors and hooks related to the matching controller here.
// All this logic will automatically be available in application.js.

$('.queries.shared').ready(function(){
  $('#query_result').dataTable({
    ordering: false,
    "dom": 'tip'
  });
});
