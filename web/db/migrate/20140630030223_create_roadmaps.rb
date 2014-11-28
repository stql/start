class CreateRoadmaps < ActiveRecord::Migration
  def change
    create_table :roadmaps do |t|
      t.text :cell_type
      t.text :experiment
      t.text :fname
      t.text :url

      t.timestamps
    end
  end
end
