class CreateSnps < ActiveRecord::Migration
  def change
    create_table :snps do |t|
      t.integer :build_number
      t.string :feature
      t.string :fname
      t.text :url, :limit => nil

      t.timestamps
    end
  end
end
