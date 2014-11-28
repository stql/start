class WgEncodeUwTfbs < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["antibody", "cell", "replicate", "set_type", "treatment", "view", "fname"]

  self.inheritance_column = nil

end