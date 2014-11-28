class WgEncodeCshlShortRnaSeq < ActiveRecord::Base
  cattr_accessor :display_columns
  default_scope { where('obj_status is null') }

  @@display_columns = ["bio_rep", "cell", "localization", "replicate", "view", "fname"]

  self.inheritance_column = nil

end