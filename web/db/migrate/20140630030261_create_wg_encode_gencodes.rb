class CreateWgEncodeGencodes < ActiveRecord::Migration
  def change
    create_table :wg_encode_gencodes do |t|
      t.text :version
      t.text :features
      t.text :fname
      t.text :url

      t.timestamps
    end
  end
end
