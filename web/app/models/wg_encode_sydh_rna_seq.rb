class WgEncodeSydhRnaSeq < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell", "rna_extract", "treatment", "view", "fname"]

  self.inheritance_column = nil

end