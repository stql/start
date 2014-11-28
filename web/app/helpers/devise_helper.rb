module DeviseHelper
  def devise_error_messages!
    return '' if resource.errors.empty?

    msg = ''
    resource.errors.full_messages.each do |single_message|
      tmp_msg = <<-HTML
      <div data-alert class="alert-box small-6 small-centered columns alert">
        <div>#{single_message}</div>
        <a href="#" class="close">&times;</a>
      </div>
      HTML
      msg += tmp_msg
    end

    html = '<div class="row" id="error_area">'+msg+'</div>'

    html.html_safe
  end
end
