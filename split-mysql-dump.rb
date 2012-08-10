#!/usr/bin/env ruby
 
require 'optparse'
 
tables = []
ignore = []
structure_only = []
dumpfile = ""

cmds = OptionParser.new do |opts|
  opts.banner = "Usage: split-mysql-dump.rb [options] [FILE]"

  opts.on("-s", "Read from stdin") do
  dumpfile = $stdin
  end
  
  opts.on("-t", '--tables TABLES', Array, "Extract only these tables") do |t|
    tables = t
  end
  
  opts.on("-i", '--ignore-tables TABLES', Array, "Ignore these tables") do |i|
    ignore = i
  end

  opts.on("-s", '--structure-only TABLES', Array, "Ignore data from these tables") do |s|
    structure_only = s
  end
  
  opts.on_tail("-h", "--help") do
    puts opts
  end

end.parse!

if dumpfile == ""
  dumpfile = ARGV.shift
  if not dumpfile
    puts "Nothing to do"
    exit 
  end
end

STDOUT.sync = true
 
class Numeric
  def bytes_to_human
    units = %w{B KB MB GB TB}
    e = self > 0 ? (Math.log(self)/Math.log(1024)).floor : 0
    s = "%.3f" % (to_f / 1024**e)
    s.sub(/\.?0*$/, units[e])
  end
end

if File.exist?(dumpfile)
  if dumpfile == $stdin
    d = $stdin
  else
    d = File.new(dumpfile, "r")
  end
 
  outfile = nil
  table = nil
  db = nil
  ignore_data = false
  dumping = false
  linecount = tablecount = starttime = 0
 
  while (line = d.gets)
    # Must be UTF-8.
    line = line.encode("UTF-16BE", :invalid=>:replace, :replace=>"?").encode("UTF-8")
    # Detect table changes
    if line =~ /^-- Table structure for table .(.+)./ or line =~ /^-- Dumping data for table .(.+)./
      is_new_table = table != $1
      table = $1
      dumping = (line =~ /Dumping/) ? true : false

      # previous file should be closed
      if is_new_table
        outfile.close if outfile and !outfile.closed?
        puts("\n\nFound a new table: #{table}")
        ignore_data = false

        if (tables != [] and not tables.include?(table))
          puts"`#{table}` not in list, ignoring"
          table = nil
        elsif (ignore != [] and ignore.include?(table))
          puts"`#{table}` will be ignored"
          table = nil
        else
          if (structure_only != [] and structure_only.include?(table))
            puts"`#{table}` data will be ignored"
            ignore_data = true
          end
          starttime = Time.now
          linecount = 0
          tablecount += 1
          outfile = File.new("#{db}_#{table}.sql", "w")
          outfile.syswrite("USE `#{db}`;\n\n") unless db.nil?
        end
      end
    elsif line =~ /^-- Current Database: .(.+)./
      db = $1
      table = nil
      outfile.close if outfile and !outfile.closed?
      outfile = File.new("#{db}_1create.sql", "w")
      puts("\n\nFound a new db: #{db}")
    elsif line =~ /^-- Position to start replication or point-in-time recovery from/
      db = nil
      table = nil
      outfile.close if outfile and !outfile.closed?
      outfile = File.new("1replication.sql", "w")
      puts("\n\nFound replication data")
    end
 
    # Write line to outfile
    if outfile and !outfile.closed?
      outfile.syswrite(line) unless ignore_data && dumping
      linecount += 1
      elapsed = Time.now.to_i - starttime.to_i + 1
      print("    writing line: #{linecount} #{outfile.stat.size.bytes_to_human} in #{elapsed} seconds #{(outfile.stat.size / elapsed).bytes_to_human}/sec                 \r")
    end
  end
end
 
puts
