class Conservation < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["number_of_species", "clades", "fname"]

  self.inheritance_column = nil

end