class AddFileSizeToUsers < ActiveRecord::Migration
  def change
    add_column :users, :file_size, :integer, :default => 0
  end
end
