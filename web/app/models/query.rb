class Query < ActiveRecord::Base
  belongs_to :user

  validates :name, presence: true
  before_create :generate_token

  def to_param
    [id, token].join('-')
  end

  protected
  def generate_token
    self.token = loop do
      random_token = SecureRandom.urlsafe_base64(nil, false)
      break random_token unless ::Query.exists?(token: random_token)
    end
  end
end
