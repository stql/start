class WgEncodeGisChiaPet < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["antibody", "cell", "replicate", "view", "fname"]

  self.inheritance_column = nil

end