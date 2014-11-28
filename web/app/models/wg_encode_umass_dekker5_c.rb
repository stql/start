class WgEncodeUmassDekker5C < ActiveRecord::Base
  cattr_accessor :display_columns
  default_scope { where('obj_status is null') }

  @@display_columns = ["cell", "region", "replicate", "view", "fname"]

  self.inheritance_column = nil

end