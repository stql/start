class AddSharedToQuery < ActiveRecord::Migration
  def change
    add_column :queries, :shared, :boolean, :default => false
  end
end
