class FantomFive < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["biological_category", "technology", "comment__sample_name_", "parameter__rna_extraction_", "parameter__sex_", "fname"]

  self.inheritance_column = nil

end