# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ name: 'Chicago' }, { name: 'Copenhagen' }])
#   Mayor.create(name: 'Emanuel', city: cities.first)


require 'csv'

# Load the datasets
data_dir = "#{Rails.root}/db/seed/"
text = File.read(File.join(data_dir, "dataset.csv")).gsub(/\\"/, '""')
CSV.parse text, :headers => :first_row do |row|
  dataset_csv = File.join(data_dir, row['dbname']+".csv")
  @dataset = Dataset.create(row.to_hash)

  # Feed into destinated table, should be already created
  puts row['dbname']
  category_name = row['dbname'].titleize.gsub " ", ""
  CSV.foreach dataset_csv, :headers => :first_row do |data_row|
    data_hash = data_row.to_hash
    @entry = eval("#{category_name}.create(data_hash)")
  end
  puts '---'
end

# Load the default user (for non-logged-in submission)
user = User.new :email => 'sample@abc.com', :password => '28pEPwAYuxNdG98UCllNsehlUzX', :password_confirmation => '28pEPwAYuxNdG98UCllNsehlUzX', :id => 0
user.skip_confirmation!
user.save!
