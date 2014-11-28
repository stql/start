class WgEncodeUwHistone < ActiveRecord::Base
  cattr_accessor :display_columns
  default_scope { where('obj_status is null') }

  @@display_columns = ["antibody", "cell", "replicate", "set_type", "treatment", "view", "fname"]

  self.inheritance_column = nil

end