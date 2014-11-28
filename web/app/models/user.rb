require 'hive_web'

class User < ActiveRecord::Base
  # Include default devise modules. Others available are:
  # :confirmable, :lockable, :timeoutable and :omniauthable
  devise :database_authenticatable, :registerable, :confirmable,
         :recoverable, :rememberable, :trackable, :validatable

  has_many :query
  has_many :track

  before_create :create_hive_db
  before_destroy :remove_hive_db

  protected
  def create_hive_db
    self.db_name = loop do
      random_token = Array.new(8){[*'0'..'9', *'a'..'z', '_'].sample}.join
      db_name = 'user_%s' % [random_token]
      break db_name unless ::User.exists?(db_name: db_name)
    end

    hweb = HiveWeb.new
    hweb.create_user_db(self.db_name)
  end

  def remove_hive_db
    hweb = HiveWeb.new
    hweb.delete_user_db(self.db_name)
 end
end
