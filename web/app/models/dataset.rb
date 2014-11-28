require 'json'

class Dataset < ActiveRecord::Base
  after_find :parse_columns

  def self.get_entries(dataset_id)
    dataset = Dataset.find(dataset_id)
    eval("#{dataset.dbname.titleize.gsub(' ', '')}.all")
  end

  def self.get_dbname(dataset_id)
    Dataset.find(dataset_id).dbname
  end

  def self.get_data(dataset_id)
    Dataset.find(dataset_id).data
  end

  private
  def parse_columns
    self.columns = JSON.parse(self.columns)
  end
end
