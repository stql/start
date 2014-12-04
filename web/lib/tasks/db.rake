require 'csv'

namespace :db do
  desc "Update file record in database"
  task :update_dataset => :environment do
    Rake::Task['db:clear_dataset'].execute
    Rake::Task['db:write_dataset'].execute
  end

  desc "Clear file record in database"
  task :clear_dataset => :environment do
    # Clear dataset
    Dataset.destroy_all
    puts "%s is cleared." %"Dataset"

    data_dir = "#{Rails.root}/db/seed/"
    text = File.read(File.join(data_dir, "dataset.csv")).gsub(/\\"/, '""')
    CSV.parse text, :headers => :first_row do |row|
      # Clear files in category
      category_name = row['dbname'].titleize.gsub " ", ""
      eval("#{category_name}.destroy_all")

      puts "%s is cleared." %category_name
    end
  end
  
  desc "Write file record in database"
  task :write_dataset => :environment do
    # Load the datasets
    data_dir = "#{Rails.root}/db/seed/"
    text = File.read(File.join(data_dir, "dataset.csv")).gsub(/\\"/, '""')
    CSV.parse text, :headers => :first_row do |row|
      dataset_csv = File.join(data_dir, row['dbname']+".csv")
      @dataset = Dataset.create(row.to_hash)

      # Feed into destinated table, should be already created
      category_name = row['dbname'].titleize.gsub " ", ""

      CSV.foreach dataset_csv, :headers => :first_row do |data_row|
        data_hash = data_row.to_hash
        @entry = eval("#{category_name}.create(data_hash)")
      end
      puts "%s is updated." %category_name
    end
  end
end