ActionMailer::Base.smtp_settings = {
  :address => 'community.cs.hku.hk',
  :port => 587,
  :authentication => :login,
  :user_name => ENV['SMTP_USERNAME'],
  :password =>  ENV['SMTP_PASSWORD'],
  :enable_starttls_auto => true
}
