class WgEncodeBroadHmm < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell", "view", "fname"]

  self.inheritance_column = nil

end