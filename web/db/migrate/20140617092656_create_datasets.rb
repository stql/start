class CreateDatasets < ActiveRecord::Migration
  def change
    create_table :datasets do |t|
      t.text :data
      t.string :dbname
      t.text :link
      t.string :columns

      t.timestamps
    end
  end
end
