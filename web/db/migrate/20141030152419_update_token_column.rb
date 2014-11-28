class UpdateTokenColumn < ActiveRecord::Migration
  def change
    rename_column :users, :token, :db_name
  end
end
