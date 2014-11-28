class WgEncodeDukeAffyExon < ActiveRecord::Base
  cattr_accessor :display_columns
  default_scope { where('obj_status is null') }

  @@display_columns = ["cell", "replicate", "treatment", "type", "view", "fname"]

  self.inheritance_column = nil

end