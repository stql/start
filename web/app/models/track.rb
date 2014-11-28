require 'hive_web'
require 'rbconfig'
require 'open3'

class Track < ActiveRecord::Base
  cattr_accessor :display_columns
  @@display_columns = ["fname"]

  belongs_to :user

  validates :fname, uniqueness: true

  before_create :create_user_track
  before_update :rename_hive_table
  before_destroy :destroy_hive_table
  before_validation :replace_track_characters

  def self.user_tracks_size(user)
    return 0 if Track.where(user_id: user).length == 0
    
    Track.where(user_id: user).pluck(:fsize).inject(:+)
  end

  def self.replace_track_name(name)
    name.gsub(/\W/, '_').downcase
  end


  def add_user_file(file, user)
    @file = file
    @user = user
  end

  def create_user_track
    # Procedure
    # - check file extension
    ret = check_extension
    if ret != nil
      self.errors[:base] << ret
      return false
    end

    # - check file size
    ret = check_size_limit
    if ret != nil
      self.errors[:base] << ret
      return false
    end

    # - converting the file
    begin
      ret = convert_file
    rescue
      self.errors[:base] << "Error in converting file"
      return false
    end
    if ret != nil
      self.errors[:base] << ret
      return false
    end

    # - create the hive table
    ret = create_hive_table
    if ret != nil
      self.errors[:base] << ret
      return false
    end

    # - load the data
    ret = load_hive_data
    if ret != nil
      self.errors[:base] << ret
      return false
    end
  end

  def check_extension
    file_ext = file_extension
    if not ['bed', 'wig', 'bedGraph'].include?(file_ext)
      return "Only support bed, wig and bedgraph files"
    end
  end

  def check_size_limit
    user_limit = Integer(ENV['user_tracks_limit'])
    usage = self.class.user_tracks_size(@user)
    if (usage+@file.size) > user_limit
      return "Total tracks usage over the limit (%d MB)" % [user_limit/1024/1024]
    end
  end

  def convert_file
    file_type = file_extension
    file_path = @file.path.to_s

    if file_type == 'wig' # convert wig to bedgraph
      tempBedGraph = Tempfile.new('stql_bedGraph')

      fixStepToBedGraph = Rails.root.join('lib', 'assets', 'wig_conversion', 'FixStepToBedGraph.pl').to_s
      stdout, _, s = Open3.capture3(fixStepToBedGraph, file_path)
      return "Error in converting file" unless s.success?

      tempBedGraph.write(stdout)
      tempBedGraph.rewind

      # replace as default file
      zero_based_file = tempBedGraph
    else
      zero_based_file = @file.tempfile
    end

    # convert it into one based
    one_based_content = zero_based_file.read.gsub(/\t(\d+)\t.+/) { |s| 
      s.gsub!($1) { |t| 
        (Integer(t)+1).to_s 
      } 
    }
    zero_based_file.rewind

    @one_based_file = Tempfile.new('stql_oneBased')
    @one_based_file.write(one_based_content)
    @one_based_file.close
  end

  def create_hive_table
    db_name = @user.db_name
    table_name = self.fname

    hweb = HiveWeb.new
    if file_extension == 'bed'
      count = 3 
      File.open(@one_based_file.path) do |f| 
        f.each_line do |line|
          size = line.split("\t").length
          count = size if size > count
        end 
      end

      if count > 12
        return "Number of column is not match with BED format"
      end

      ret = hweb.create_hive_table(db_name, table_name, true, count)
    else
      ret = hweb.create_hive_table(db_name, table_name)
    end

    return ret["error"] if ret.has_key?("error")
  end

  def load_hive_data
    db_name = self.user.db_name
    table_name = self.fname
    file_path = @one_based_file.path
    
    hweb = HiveWeb.new
    ret = hweb.load_hive_data(db_name, table_name, file_path)
    return "Error in loading data into hive" if !ret
  end

  def destroy_hive_table
    db_name = self.user.db_name
    table_name = self.fname

    hweb = HiveWeb.new
    ret = hweb.delete_hive_table(db_name, table_name)

    if ret.has_key?("error")
      self.errors[:base] << ret["error"]
      return false
    end
  end

  def rename_hive_table
    db_name = self.user.db_name
    old_name = self.fname_was
    new_name = self.fname

    hweb = HiveWeb.new
    ret = hweb.rename_hive_table(db_name, old_name, new_name)

    if ret.has_key?("error")
      self.errors[:base] << ret["error"]
      return false
    end
  end

  protected
  def file_extension
    @file.original_filename.split('.')[-1].downcase
  end

  def replace_track_characters
    self.fname = self.class.replace_track_name(self.fname)
  end
end
