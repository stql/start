class WgEncodeAffyRnaChip < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell", "localization", "rna_extract", "view", "fname"]

  self.inheritance_column = nil

end