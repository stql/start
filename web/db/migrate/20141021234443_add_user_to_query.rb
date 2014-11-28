class AddUserToQuery < ActiveRecord::Migration
  def change
    add_reference :queries, :user, index: true, default: 0
  end
end
