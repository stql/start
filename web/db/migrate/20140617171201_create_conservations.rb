class CreateConservations < ActiveRecord::Migration
  def change
    create_table :conservations do |t|
      t.integer :number_of_species
      t.string :clades
      t.string :fname
      t.text :url, :limit => nil

      t.timestamps
    end
  end
end
