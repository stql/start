class AddSizeOnTrack < ActiveRecord::Migration
  def change
    add_column :tracks, :fsize, :integer
  end
end
