require 'json'
require 'open3'

class HiveWeb
  @@REST_TIMEOUT = 2 # Wait for 2 seconds on request
  @@USER_NAME = 'fwang'
  @@WebHCat_URL = 'http://galaxy013:50111/templeton/v1/'
  @@WebHDFS_URL = 'http://galaxy013:50070/webhdfs/v1/'
  @@HDFS_DB_URL = 'hdfs://galaxy013:9000/user/hive/user_warehouse/'


  # Class method, mainly urls
  def self.statusdir (query_id)
    "stql_%s_%d" % [ENV['RAILS_ENV'], query_id]
  end

  def self.hive_url
    @@WebHCat_URL+'hive/'
  end

  def self.queue_url (job_id)
    @@WebHCat_URL+'queue/'+job_id+'?user.name='+@@USER_NAME
  end

  def self.stdout_url (query_id)
    @@WebHDFS_URL+'user/'+@@USER_NAME+'/'+statusdir(query_id)+'/stdout?op=OPEN'
  end

  def self.stderr_url (query_id)
    @@WebHDFS_URL+'user/'+@@USER_NAME+'/'+statusdir(query_id)+'/stderr?op=OPEN'
  end

  def self.user_db_url (db_name)
    # 'http://galaxy013:50111/templeton/v1/ddl/database/newdb?user.name=fwang' 
    @@WebHCat_URL+'ddl/database/'+db_name+'?user.name='+@@USER_NAME
  end

  def self.user_table_url(db_name, table_name)
    # 'http://galaxy013:50111/templeton/v1/ddl/database/newdb/table/newtable?user.name=fwang'
    @@WebHCat_URL+'ddl/database/'+db_name+'/table/'+table_name+'?user.name='+@@USER_NAME
  end


  # Instance method, for query, database, tracks
  def make_query(user_query, query_id)
    c = create_curl_req(self.class.hive_url)
    c.http_post(Curl::PostField.content('user.name', @@USER_NAME),
                Curl::PostField.content('statusdir', self.class.statusdir(query_id)),
                Curl::PostField.content('execute', user_query))
    c
  end

  def query_status(job_id)
    _query_status(job_id)
  end

  def is_query_completed(job_id)
    c = _query_status(job_id)

    response = JSON.parse(c.body_str)
    response['completed'] != nil
  end

  def query_stdout(query_id)
    stdout_url = self.class.stdout_url(query_id)
    c = create_curl_req(stdout_url)
    c.perform
    c
  end

  def query_stderr(query_id)
    stderr_url = self.class.stderr_url(query_id)
    c = create_curl_req(stderr_url)
    c.perform
    c
  end

  def create_user_db(db_name)
    hive_location = @@HDFS_DB_URL+db_name+'.db'
    put_data = {:location => hive_location}

    dst_url = self.class.user_db_url(db_name)
    c = create_curl_req_with_data(dst_url)
    c.http_put(put_data.to_json)
    JSON.parse(c.body_str)
  end

  def delete_user_db(db_name)
    dst_url = self.class.user_db_url(db_name)+'&option=cascade'
    c = create_curl_req(dst_url)
    c.http_delete()
    JSON.parse(c.body_str)
  end

  def create_hive_table(db_name, table_name, is_bed=false, column_num=3)
    put_data = { 
      :columns => [
        { :name => "chr", :type => "string" },
        { :name => "chrstart", :type => "int" },
        { :name => "chrend", :type => "int" }
      ],
      :format => {
        :storedAs => "textfile",
        :rowFormat => {
          :fieldsTerminatedBy => "\t",
          :linesTerminatedBy => "\n"
        }
      }
    }

    if is_bed
      column_num -= 4
      for i in 0..column_num
        column_name = ("a".ord+i).chr

        put_data[:columns].push({:name => column_name, :type => "string"})
      end
    else
      put_data[:columns].push({:name => "value", :type => "float"})
    end

    dst_url = self.class.user_table_url(db_name, table_name)
    c = create_curl_req_with_data(dst_url)
    c.http_put(put_data.to_json)
    JSON.parse(c.body_str)
  end

  def load_hive_data(db_name, table_name, file_path)
    # curl -X PUT -L -T full_bed "http://galaxy013:50070/webhdfs/v1/user/hive/user_warehouse/user_j5d_awbr.db/full_bed/t1?user.name=fwang&op=CREATE"
    url = @@WebHDFS_URL+'user/hive/user_warehouse/'+db_name+'.db/'+table_name+'/'+table_name+'?user.name='+@@USER_NAME+'&op=CREATE'
    stdout, stderr, s = Open3.capture3('curl', '-X', 'PUT', '-L', '-T', file_path, url)

    return (stdout.size == 0) && (s.success?)
  end

  def delete_hive_table(db_name, table_name)
    dst_url = self.class.user_table_url(db_name, table_name)
    c = create_curl_req(dst_url)
    c.http_delete()
    JSON.parse(c.body_str)
  end

  def rename_hive_table(db_name, old_name, new_name)
    post_data = {:rename => new_name}

    dst_url = self.class.user_table_url(db_name, old_name)
    c = create_curl_req(dst_url)
    c.http_post(Curl::PostField.content('rename', new_name))
    JSON.parse(c.body_str)
  end

  private
  def create_curl_req(url)
    Curl::Easy.new(url) do |curl|
      curl.follow_location = true
      curl.connect_timeout = @@REST_TIMEOUT
    end 
  end

  def create_curl_req_with_data(url)
    Curl::Easy.new(url) do |curl|
      curl.follow_location = true
      curl.connect_timeout = @@REST_TIMEOUT
      curl.headers['Content-type'] = 'application/json'
    end
  end

  def _query_status(job_id)
    queue_url = self.class.queue_url(job_id)
    c = create_curl_req(queue_url)
    c.perform
    c
  end
end