class Roadmap < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell_type", "experiment", "fname"]

  self.inheritance_column = nil

end