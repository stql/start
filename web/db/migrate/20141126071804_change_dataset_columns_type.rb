class ChangeDatasetColumnsType < ActiveRecord::Migration
  def change
    reversible do |dir|
      dir.up do
        change_column :datasets, :columns, :text
      end
      dir.down do
        change_column :datasets, :columns, :string
      end
    end
  end
end
