$('.queries.new').ready(function(){
  myCodeMirror = CodeMirror.fromTextArea(document.getElementById("query_user_query"), {
    mode:  "text/x-hive",
    lineWrapping: true,
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets : true,
    autofocus: true
  });
  myCodeMirror.setSize(null, myCodeMirror.defaultTextHeight()*10+8);

  $('#sticky').sticky({
    topSpacing: 45
  })
  .on('sticky-start', function(){
    var $par = $(this).parent(); 
    $(this).width($par.width()); 
  });

  $('#items_grid').on('click', 'table td', function(evt){
    var dbName = $(this).parents('table').data('dbname');
    var tableName = $(this).parent().children().last().html();
    var fullName = '`'+dbName+'`.`'+tableName+'`';

    // Add in the end
    // myCodeMirror.replaceRange(' '+fullName+' ', CodeMirror.Pos(myCodeMirror.lastLine()));

    // Add in the current cursor position
    myCodeMirror.replaceRange(' '+fullName+' ', myCodeMirror.getCursor());

    myCodeMirror.focus();
  });

  $('#new_query').on('ajax:success', function(evt, data, status, xhr){
    console.log(data);
    if (data.status == 'success') {
      window.location = data.url;
    }
    else {
      // Error msg
      var errorDiv = '<div data-alert class="error alert alert-box">'+data.msg+'<a href="#" class="close">&times;</a></div>';
      $('#error_area').html('').append(errorDiv).foundation();
    }
  });

});