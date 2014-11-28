class WgEncodeUwRepliSeq < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell", "replicate", "view", "fname"]

  self.inheritance_column = nil

end