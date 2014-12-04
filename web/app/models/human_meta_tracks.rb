class HumanMetaTracks < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell_type", "region_type", "other", "fname"]

  self.inheritance_column = nil

end
