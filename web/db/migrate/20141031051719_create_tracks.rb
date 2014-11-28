class CreateTracks < ActiveRecord::Migration
  def change
    create_table :tracks do |t|
      t.text :fname
      t.references :user, index: true

      t.timestamps
    end
  end
end
