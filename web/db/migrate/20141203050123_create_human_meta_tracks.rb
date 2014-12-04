class CreateHumanMetaTracks < ActiveRecord::Migration
  def change
    create_table :human_meta_tracks do |t|
      t.text :cell_type
      t.text :region_type
      t.text :other
      t.text :fname
      t.text :internal_name
      t.text :url

      t.timestamps
    end
  end
end
