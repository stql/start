class CreateFantomFives < ActiveRecord::Migration
  def change
    create_table :fantom_fives do |t|
      t.text :biological_category
      t.text :technology
      t.text :extract_name
      t.text :comment__rna_tube_
      t.text :comment__sample_name_
      t.text :comment__organism_
      t.text :protocol_ref
      t.text :parameter__rna_extraction_
      t.text :extract_name_1
      t.text :comment__rna_id_
      t.text :comment__material_type_
      t.text :comment__comment_on_rna_
      t.text :protocol_ref_1
      t.text :comment__lsid_
      t.text :parameter__library_protocol_
      t.text :library_name
      t.text :comment__library_id_
      t.text :protocol_ref_2
      t.text :parameter__sequence_protocol_
      t.text :parameter__machine_name_
      t.text :parameter__run_name_
      t.text :parameter__flowcell_channel_
      t.text :file_name
      t.text :comment__sequence_raw_file_
      t.text :protocol_ref_3
      t.text :parameter__sex_
      t.text :file_name_1
      t.text :fname
      t.text :url

      t.timestamps
    end
  end
end
