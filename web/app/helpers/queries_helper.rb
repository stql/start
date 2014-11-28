module QueriesHelper
  def get_run_status(num)
    status_arr = ['', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PREP', 'KILLED']
    if num < 1 || num > status_arr.length
      return 'UNKNOWN'
    end
    status_arr[num]
  end
end
