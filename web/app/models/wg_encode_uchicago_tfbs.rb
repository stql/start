class WgEncodeUchicagoTfbs < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["antibody", "cell", "control", "replicate", "set_type", "view", "fname"]

  self.inheritance_column = nil

end