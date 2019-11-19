require 'time'
require 'pp'
require 'open3'

ENV['TZ'] = 'UTC'
DEFAULT_ENDPOINT = ''

class Param
  def self.from_json(params)
    parsed = JSON.parse(params.split(' ')[1])
    new(parsed['last_session_time'], parsed['next_session_time'], parsed['last_executed_session_time'])
  end
  
  def initialize(last_session_time, next_session_time, last_executed_session_time)
    @last_session_time = Time.parse(last_session_time)
    @next_session_time = Time.parse(next_session_time)
    @last_executed_session_time = Time.parse(last_executed_session_time)
  end

  def retried?
    !(@last_session_time == @last_executed_session_time)
  end
end

class Session
  def initialize(id, attempt_id, uuid, project, workflow, session_time, retry_attempt_name, params, created_at, kill_requested, status)
    @id = strip_param(id).to_i
    @attempt_id = strip_param(attempt_id).to_i
    @uuid = strip_param(uuid)
    @project = strip_param(project)
    @workflow = strip_param(workflow)
    @session_time = Time.parse(strip_param(session_time, true))
    @retry_attempt_name = strip_param(retry_attempt_name)
    @params = Param.from_json(params)
    @created_at = Time.parse(strip_param(created_at, true))
    @kill_requested = strip_param(kill_requested)
    @status = strip_param(status)
  end

  attr_reader :session_time, :id, :status, :attempt_id

  def short_desc
    "#{@id}, #{attempt_id}, #{@session_time}, #{@status}, #{retried?}"
  end

  def retried?
    @params.retried?
  end

  def succeeded?
    @status == 'success'
  end

  def failed?
    @status == 'error'
  end

  def running?
    @status == 'running'
  end

  private

  def strip_param(param, is_time = false)
    if is_time
      s = param.split(/at:|time:/)
    else
      s = param.split(':')
    end
    s.length > 1 ? s[1].strip : ''    
  end
end

class DigdagClient
  def initialize(project, workflow, endpoint = DEFAULT_ENDPOINT, page_size)
    @project = project
    @workflow = workflow    
    @endpoint = endpoint
    @page_size = page_size
  end

  def sessions_until(time_to)
    exec_until(time_to)
  end

  def failed_sessions_until(time_to)
    sessions_until(time_to).select {|session| session.failed? }
  end

  private

  # do while untile time_to
  def exec_until(time_to)
    results = exec.reverse
    while (results.last.session_time > time_to)
      results += exec(results.last.id).reverse
    end
    results
  end

  def exec(last_id = nil)
    endpoint_option = "-e #{@endpoint}"
    last_id_option = "-i #{last_id}" if last_id
    page_size_option = "-s #{@page_size}"
    cmd = ['digdag sessions', @project, @workflow, endpoint_option, last_id_option, page_size_option].join(' ')
    puts "Exec comannd: #{cmd}"

    result = ''
    Open3.popen3(cmd) do |stdin, stdout, stderr, wait_thr|
      result = stdout.read  
    end

    entries = result.split("\n\n").map {|e| e.split("\n") }.map {|a| a.map(&:strip) }
    entries[0].shift
    results = entries.map do |entry|    
      Session.new(*entry)
    end
    results
  end

end

def main(argv)
  endpoint = argv[0]
  project = argv[1]
  workflow = argv[2]
  # time_from = Time.parse(argv[3])
  time_to = Time.parse(argv[3])
  # last_id = ARGV[5]

  page_size = 30

  # if time_from > time_to
  #   STDERR.puts 'Time configuration is wrong'
  #   exit 1
  # end

  client = DigdagClient.new(project, workflow, endpoint, page_size)
  # sessions = client.sessions_until(time_to)
  sessions = client.failed_sessions_until(time_to)
  pp sessions.map(&:short_desc)
end

main(ARGV)
