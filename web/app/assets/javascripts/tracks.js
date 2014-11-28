// Place all the behaviors and hooks related to the matching controller here.
// All this logic will automatically be available in application.js.
$('a[data-class="track_rename"]').on('click', function(){
  var currentName = $(this).parent().siblings(':first').html();
  var updatedName = prompt("Please enter new table name \n(non-alphanumeric characters are replaced as '_')", currentName);

  if(updatedName != null && updatedName != currentName){
    // Send request to update controller
    var url = $(this).attr('data-path');
    $.ajax({
      type: 'PUT',
      url: url,
      data: JSON.stringify({
        track: {fname: updatedName}
      }),
      contentType: 'application/json',
      dataType: 'json',
      success: function() {
        location.reload(true);
      }
    });
  }
});