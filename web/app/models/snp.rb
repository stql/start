class Snp < ActiveRecord::Base
  # after_find :hide_columns
  cattr_accessor :display_columns

  @@display_columns = ["build_number", "feature", "fname"]

  self.inheritance_column = nil

end
