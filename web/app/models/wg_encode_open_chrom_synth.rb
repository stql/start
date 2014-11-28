class WgEncodeOpenChromSynth < ActiveRecord::Base
  cattr_accessor :display_columns

  @@display_columns = ["cell", "treatment", "view", "fname"]

  self.inheritance_column = nil

end