class WgEncodeGisRnaSeq < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["bio_rep", "cell", "localization", "replicate", "rna_extract", "view", "fname"]

  self.inheritance_column = nil

end