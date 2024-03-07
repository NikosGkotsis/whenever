module Whenever
  class JobList
    attr_reader :roles

    def initialize(options)
      @jobs, @env, @set_variables, @pre_set_variables = {}, {}, {}, {}

      if options.is_a? String
        options = { :string => options }
      end

      pre_set(options[:set])

      @roles = options[:roles] || []

      @yaml_path = options[:yaml_path] || "./crontab.yaml"

      setup_file = File.expand_path('../setup.rb', __FILE__)
      setup = File.read(setup_file)
      schedule = if options[:string]
        options[:string]
      elsif options[:file]
        File.read(options[:file])
      end

      instance_eval(setup, setup_file)
      instance_eval(schedule, options[:file] || '<eval>')
    end

    def set(variable, value)
      variable = variable.to_sym
      return if @pre_set_variables[variable]

      instance_variable_set("@#{variable}".to_sym, value)
      @set_variables[variable] = value
    end

    def method_missing(name, *args, &block)
      @set_variables.has_key?(name) ? @set_variables[name] : super
    end

    def self.respond_to?(name, include_private = false)
      @set_variables.has_key?(name) || super
    end

    def env(variable, value)
      @env[variable.to_s] = value
    end

    def every(frequency, options = {})
      @current_time_scope = frequency
      @options = options
      yield
    end

    def job_type(name, template)
      singleton_class.class_eval do
        define_method(name) do |task, *args|
          options = { :task => task, :template => template }
          options.merge!(args[0]) if args[0].is_a? Hash

          options[:mailto] ||= @options.fetch(:mailto, :default_mailto)

          # :cron_log was an old option for output redirection, it remains for backwards compatibility
          options[:output] = (options[:cron_log] || @cron_log) if defined?(@cron_log) || options.has_key?(:cron_log)
          # :output is the newer, more flexible option.
          options[:output] = @output if defined?(@output) && !options.has_key?(:output)

          @jobs[options.fetch(:mailto)] ||= {}
          @jobs[options.fetch(:mailto)][@current_time_scope] ||= []
          @jobs[options.fetch(:mailto)][@current_time_scope] << Whenever::Job.new(@options.merge(@set_variables).merge(options))
        end
      end
    end

    def generate_cron_output
      [environment_variables, cron_jobs].compact.join
    end

    # Generates aYAML with crons.YAML may look like following:
    # The YAML will look like following
    #     - name: CategoryCuratedKeyphrase.refresh_last_crawled_at
    #       env: RAILS_RUNNER_NAME='\''runner CategoryCuratedKeyphrase.refresh_last_crawled_at'\''
    #       command: exec rails runner -e development '\''Skroutz.run_cron("CategoryCuratedKeyphrase.refresh_last_crawled_at")
    #         { CategoryCuratedKeyphrase.refresh_last_crawled_at }'\''
    #       time: 30 2 * * *
    #     - name: mailer:prompt_for_shopping_feedback
    #       env: ''
    #       command: exec rake mailer:prompt_for_shopping_feedback
    #       time: 0 10 * * *
    def generate_yaml_output
      return if @jobs.empty?

      output = []
      @jobs.each do |mailto, time_and_jobs|
        output_jobs = []

        time_and_jobs.each do |time, jobs|
          output_jobs << yaml_cron_jobs_of_time(time, jobs)
        end

        output_jobs.reject! { |output_job| output_job.empty? }

        output << output_jobs
      end
      y ={'cronjobs'=>output[0][0]}
      File.open(@yaml_path, 'w') {|f| f.write y.to_yaml }
    end

  private

    #
    # Takes a string like: "variable1=something&variable2=somethingelse"
    # and breaks it into variable/value pairs. Used for setting variables at runtime from the command line.
    # Only works for setting values as strings.
    #
    def pre_set(variable_string = nil)
      return if variable_string.nil? || variable_string == ""

      pairs = variable_string.split('&')
      pairs.each do |pair|
        next unless pair.index('=')
        variable, value = *pair.split('=')
        unless variable.nil? || variable == "" || value.nil? || value == ""
          variable = variable.strip.to_sym
          set(variable, value.strip)
          @pre_set_variables[variable] = value
        end
      end
    end

    def environment_variables
      return if @env.empty?

      output = []
      @env.each do |key, val|
        output << "#{key}=#{val.nil? || val == "" ? '""' : val}\n"
      end
      output << "\n"

      output.join
    end

    #
    # Takes the standard cron output that Whenever generates and finds
    # similar entries that can be combined. For example: If a job should run
    # at 3:02am and 4:02am, instead of creating two jobs this method combines
    # them into one that runs on the 2nd minute at the 3rd and 4th hour.
    #
    def combine(entries)
      entries.map! { |entry| entry.split(/ +/, 6) }
      0.upto(4) do |f|
        (entries.length-1).downto(1) do |i|
          next if entries[i][f] == '*'
          comparison = entries[i][0...f] + entries[i][f+1..-1]
          (i-1).downto(0) do |j|
            next if entries[j][f] == '*'
            if comparison == entries[j][0...f] + entries[j][f+1..-1]
              entries[j][f] += ',' + entries[i][f]
              entries.delete_at(i)
              break
            end
          end
        end
      end

      entries.map { |entry| entry.join(' ') }
    end

    def cron_jobs_of_time(time, jobs)
      shortcut_jobs, regular_jobs = [], []

      jobs.each do |job|
        next unless roles.empty? || roles.any? do |r|
          job.has_role?(r)
        end
        Whenever::Output::Cron.output(time, job, :chronic_options => @chronic_options) do |cron|
          cron << "\n\n"

          if cron[0,1] == "@"
            shortcut_jobs << cron
          else
            regular_jobs << cron
          end
        end
      end

      shortcut_jobs.join + combine(regular_jobs).join
    end

    # Returns a list with hashes to populate the YAML.
    # Each job in YAML will contain a name, env vars for the job,
    # the actual command that the job executes and the time of schedule.
    def yaml_cron_jobs_of_time(time, jobs)
      list = []
      id = 0
      jobs.each do |job|
        next unless roles.empty? || roles.any? do |r|
          job.has_role?(r)
        end
        id += 1
        Whenever::Output::Cron.yaml_output(time, job, :chronic_options => @chronic_options) do |time, command|
          formatted_command = format_command(command)
          command_parts = formatted_command.split
          # formatted_command may look like following
          # RAILS_RUNNER_NAME='\''runner CategoryCuratedKeyphrase.refresh_last_crawled_at'\''
          #  bundle exec rails runner -e development '\''Skroutz.run_cron("CategoryCuratedKeyphrase.refresh_last_crawled_at")
          #  { CategoryCuratedKeyphrase.refresh_last_crawled_at }'\''
          #
          # The produced yaml should look like below, populate job entry accordingly.
          # - name: CategoryCuratedKeyphrase.refresh_last_crawled_at
          #   RAILS_RUNNER: CategoryCuratedKeyphrase.refresh_last_crawled_at
          #   command: exec rails runner -e development '\''Skroutz.run_cron("CategoryCuratedKeyphrase.refresh_last_crawled_at")
          #            { CategoryCuratedKeyphrase.refresh_last_crawled_at }'\''
          if formatted_command.include? 'RAILS_RUNNER_NAME'
            rails_runner = command_parts.second[0...-4]
            formatted_command = command_parts.drop(3).join(' ')
            list.append({'id'=> id, 'command'=> rails_runner, 'time'=> time, 'RAILS_RUNNER'=> rails_runner})
            next
          end
          # formatted_command will look like following
          # exec rake insights:search_session_analysis
          list.append({'id'=> id, 'command'=> formatted_command, 'time'=> time})
        end
      end
      list
    end

    def format_command(command)
      # Command will look like either of the following, remove unnecesary parts
      # /bin/bash -l -c 'cd /var/sites/skroutz_cap/current &&
      # RAILS_RUNNER_NAME='\''runner CategoryCuratedKeyphrase.refresh_last_crawled_at'\''
      #  bundle exec rails runner -e development
      #  '\''Skroutz.run_cron("CategoryCuratedKeyphrase.refresh_last_crawled_at")
      #  { CategoryCuratedKeyphrase.refresh_last_crawled_at }'\''
      #
      # /bin/bash -l -c 'cd /var/sites/skroutz_cap/current &&
      #  RAILS_ENV=development bundle exec rake search:stemming_exceptions:update_backend --silent > /dev/null'
      command.slice! "/bin/bash -l -c 'cd /var/sites/skroutz_cap/current && "
      command.slice! "RAILS_ENV=production bundle "
      command.slice! "RAILS_ENV=development bundle "
      command.slice! " --silent "
      command.slice! "> /dev/null'"
      command
    end

    def cron_jobs
      return if @jobs.empty?

      output = []

      # jobs with default mailto's must be output before the ones with non-default mailto's.
      @jobs.delete(:default_mailto) { Hash.new }.each do |time, jobs|
        output << cron_jobs_of_time(time, jobs)
      end

      @jobs.each do |mailto, time_and_jobs|
        output_jobs = []

        time_and_jobs.each do |time, jobs|
          output_jobs << cron_jobs_of_time(time, jobs)
        end

        output_jobs.reject! { |output_job| output_job.empty? }

        output << "MAILTO=#{mailto}\n\n" unless output_jobs.empty?
        output << output_jobs
      end

      output.join
    end
  end
end
