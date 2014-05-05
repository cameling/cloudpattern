# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/outputs/file"

# Spooling output for Flume
# This output will write events to files in a temporary directory.
# When files reach a given size, they are rotated to a spooling directory.
# From the directory, events are sent to HDFS by Flume.
# This output is based on file output.

class LogStash::Outputs::Spooling < LogStash::Outputs::File
  
  config_name "spooling"
  milestone 1
  
  # Flume spooling directory.
  # The files in the buffer directory (:path) are rotated to it when max size
  # (:max_size) is reached. 
  # The files are kept the same name but added with time stamp when rotating.
  config :spooling_dir, :validate => :string, :required => true

  public
  def receive(event)
    return unless output?(event)
    
    # Check if we should rotate the file.
    path = event.sprintf(@path)
    max_size = event.sprintf(@max_size).to_i
    spooling_dir = event.sprintf(@spooling_dir)
    base_name = File.basename(path)
    
    if File.exist?(path) and File.size(path) >= max_size
      close_rotating_file(path)
      time_stamp = Time.now.strftime("%Y-%m-%d_%H:%M:%S")
      new_file_name  = "#{spooling_dir}/#{base_name}_#{time_stamp}"
      File.rename(path, new_file_name)
    end

    if @message_format
      output = event.sprintf(@message_format)
    else
      output = event.to_json
    end
    
    fd = open(path)
    fd.write(output)
    fd.write("\n")

    flush(fd)
    close_stale_files
  end # def receive

  # Close files those to be rotated
  private
  def close_rotating_file(file_path)
    @files.each do |path, fd|
      if path == file_path
        flush(fd)
        fd.close
        @files.delete(path)
      end
    end
  end #def close_rotating_file
  
end # class LogStash::Outputs::Spooling