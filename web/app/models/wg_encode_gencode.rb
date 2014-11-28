class WgEncodeGencode < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["version", "features", "fname"]

  self.inheritance_column = nil

end