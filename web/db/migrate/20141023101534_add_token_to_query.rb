class AddTokenToQuery < ActiveRecord::Migration
  def change
    add_column :queries, :token, :string, :null => false
  end
end
