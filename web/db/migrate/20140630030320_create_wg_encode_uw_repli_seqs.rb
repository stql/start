class CreateWgEncodeUwRepliSeqs < ActiveRecord::Migration
  def change
    create_table :wg_encode_uw_repli_seqs do |t|
      t.text :cell
      t.text :composite
      t.text :data_type
      t.text :data_version
      t.text :date_submitted
      t.text :date_unrestricted
      t.text :dcc_accession
      t.text :geo_sample_accession
      t.text :grant
      t.text :lab
      t.text :lab_exp_id
      t.text :md5sum
      t.text :orig_assembly
      t.text :phase
      t.text :project
      t.text :replicate
      t.text :sex
      t.text :size
      t.text :sub_id
      t.text :table_name
      t.text :type
      t.text :view
      t.text :filename
      t.text :fname
      t.text :url

      t.timestamps
    end
  end
end
