class CreateQueries < ActiveRecord::Migration
  def change
    create_table :queries do |t|
      t.string :job_id
      t.text :user_query

      t.timestamps
    end
  end
end
