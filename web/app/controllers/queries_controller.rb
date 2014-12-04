require 'csv'
require 'open3'
require 'hive_web'
require 'rubygems'
require 'zip'

class QueriesController < ApplicationController
  before_action :authenticate_user!, only: [:index, :toggle_share]

  def index
    @queries = Query.where(user: current_user).order(created_at: :desc)
  end

  def new
    logger.info 'queries#new'

    @query = Query.new
    @query.user_query = 'SELECT * FROM snp.snp135common LIMIT 10;'

    if defined? params and params.has_key?(:token)
      @old_query = Query.where(token: params[:token])
      if (@old_query.size == 1 and @old_query.first.shared) \
        or @old_query.first.user == current_user

        @query.user_query = @old_query.first.user_query
      else
        flash[:query_error] = 'Invalid token'
      end
    end

    @datasets = Dataset.all
  end

  def create
    logger.info 'queries#create'

    # Parse the user_query
    user_query = String.new(params[:query][:user_query])
    name = String.new(params[:query][:name])

    # query checker
    if user_query.empty? or name.empty?
      error_msg = "Query and name cannot be empty!"
    else
      user_query = to_hive_table(user_query)
      error_msg = check_query(user_query)
    end

    # Syntax checking pass
    if error_msg.empty?
      # Save submission
      @query = Query.new(query_params)
      @query.user = current_user if current_user

      if !@query.save
        error_msg = @query.errors.to_a[0]
      else
        # replace into HIVE table name
        logger.info "mapped query: " + user_query

        # Create a request
        hweb = HiveWeb.new
        c = hweb.make_query(user_query, @query.id)

        # Parse the response
        return response_error_status(c) if c.response_code != 200
        
        # Save into db
        body = JSON.parse(c.body_str)
        @query.update(job_id: body["id"])
      end
    end

    # Return in JSON
    respond_to do |format|
      if error_msg.empty?
        msg = {:status => :success, :url => query_path(@query)} 
        format.json {render :json => msg }
      else  
        msg = {:status => :error, :msg => error_msg} 
        format.json {render :json => msg }
      end
    end
  end

  def show
    @query = Query.find(params[:id])

    # Check user
    if @query.user_id != 0 and @query.user != current_user
      return redirect_to queries_path, :alert => 'Please login with correct user account.'
    end

    # Create a request
    hweb = HiveWeb.new
    c = hweb.query_status(@query.job_id)

    logger.info c.response_code
    logger.info c.body_str

    # Parse the response
    return response_error_status(c) if c.response_code != 200
    
    @response = JSON.parse(c.body_str)
    logger.info @response['completed']
    logger.info @response['status']['runState']
  end

  def result
    @query = Query.find(params[:id])

    # Check if it is completed
    hweb = HiveWeb.new
    unless hweb.is_query_completed(@query.job_id)
      redirect_to @query
    end

    # Get stdout
    c = hweb.query_stdout(@query.id)
    return response_error_status(c) if c.response_code != 200

    @content = c.body_str

    respond_to do |format|
      format.html { generate_preview }
      format.txt { generate_txt }
      format.bed { generate_bed }
    end
  end

  def stderr
    @query = Query.find(params[:id])

    # Check if it is completed
    hweb = HiveWeb.new
    unless hweb.is_query_completed(@query.job_id)
      redirect_to @query
    end

    # Get stderr
    c = hweb.query_stderr(@query.id)
    return response_error_status(c) if c.response_code != 200

    @result = c.body_str
  end

  def toggle_share
    @query = current_user.query.find(params[:id]) 
    @query.toggle(:shared)
    if @query.save
      msg = '%s can be accessed %s.' % [@query.token, 
                                        @query.shared ? "in public": "after you've logged in"]
      redirect_to(:back, {:notice => msg })
    end
  end

  def shared
    @queries = Query.where(shared: true).where.not(user_id: 0)
    if current_user
      @queries = @queries.where.not(user: current_user)
    end
  end

  private 
  def query_params
    params.require(:query).permit(:user_query, :name)
  end

  def response_error_status(curl)
    render plain: curl.response_code
  end

  # Map fname into internal hive table name
  def to_hive_table(query)
    mapping_path = Rails.root.join('lib', 'assets', 'fname_table_mapping.tsv')
    CSV.foreach mapping_path, :col_sep => "\t" do |row|
      query.gsub!(row[0], row[1])
    end
    query
  end

  # Check query 
  def check_query(query)
    checker_path = Rails.root.join('lib', 'assets', 'Checker.jar').to_s
    stdout, stderr, status = Open3.capture3('java', '-jar', checker_path, query)

    logger.info 'check_query() checker_path: '+checker_path
    logger.info 'check_query() stdout: '+stdout
    logger.info 'check_query() stderr: '+stderr

    if stdout.eql?("OK\n")
      return ''
    end
    return stdout
  end

  def generate_preview
    original_size = @content.size
    @content = @content[0, 1024].gsub(/.+\z/, '')

    if @content.size < original_size
      @is_trimmed = true
    end
  end

  def generate_txt
    t = Tempfile.new('stql_download_txt')
    begin 
      Zip::OutputStream.open(t) { |z| 
        z.put_next_entry("%s.txt" % [@query.to_param])
        z.puts @content
      }

      send_data(IO.read(t.path), filename: "%s.txt.zip" % [@query.to_param], type: 'application/zip')
    ensure
      t.close
      t.unlink
    end
  end

  def generate_bed
    @content = @content.gsub(/\t(\d+)\t.+/) { |s| 
      s.gsub!($1) { |t| 
        (Integer(t)-1).to_s 
      } 
    }

    t = Tempfile.new('stql_download_bed')
    begin 
      Zip::OutputStream.open(t) { |z| 
        z.put_next_entry("%s.bed" % [@query.to_param])
        z.puts @content
      }

      send_data(IO.read(t.path), filename: "%s.bed.zip" % [@query.to_param], type: 'application/zip')
    ensure
      t.close
      t.unlink
    end
  end
end