module Fluent

  class HBaseOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('hbasecsv', self)

    def initialize
      super
      require 'massive_record'
      require 'time'
    end

    # Format dates with ISO 8606 by default
    # http://www.w3.org/TR/NOTE-datetime
    config_param :time_format, :string, :default => '%Y-%m-%dT%H:%M:%S.%L%:z'

    include SetTagKeyMixin
    config_set_default :include_tag_key, false
    config_set_default :tag_key, nil

    include SetTimeKeyMixin
    config_set_default :include_time_key, false
    config_set_default :time_key, nil

    config_param :tag_column_name, :string, :default => nil
    config_param :time_column_name, :string, :default => nil
    config_param :fields_to_columns_mapping, :string
    config_param :hbase_host, :string, :default => 'localhost'
    config_param :hbase_port, :integer, :default => 9090
    config_param :hbase_table, :string
    config_param :errorlog_path, :string, :default => '/home/' 

    def configure(conf)
      super

      @fields_to_columns = @fields_to_columns_mapping.split(",").map { |src_to_dst|
        src_to_dst.split("=>")
      }
      @mapping = Hash[*@fields_to_columns.flatten]
    end

    def start
      super

      @conn = MassiveRecord::Wrapper::Connection.new(:host => @hbase_host, :port => @hbase_port)
    end

    def format(tag, time, record)
      row_values = {}
      row_values[@tag_column_name] = tag unless @tag_column_name.nil?
      row_values[@time_column_name] = time unless @time_column_name.nil?
      @fields_to_columns.each {|field,column|
        next if field.nil? or column.nil?

        components = field.split(".")
        value = record
        for c in components
          value = value[c]

          break if value.nil?
        end
        row_values[column] = value
      }
      row_values.to_msgpack
    end

    def randnum(len)
       chars=("0".."9").to_a
       newnum=""
        1.upto(len) { |i| newnum << chars[rand(chars.size-1)] }
       return newnum
    end


    def write(chunk)
       chunk.msgpack_each {|row_values|
        logvalue=row_values["info:logvalue"].split(",")
        tbname=row_values["info:logpath"].split("/").last.split(".csv").first 
        if(logvalue.include?("gid"))
          @newcol=logvalue.map{|key|"info:"+key}
        else
           @newval=logvalue
        end
         unless(@newval.nil?)
           row_values2=Hash[*@newcol.zip(@newval).flatten]
         end
         unless(row_values2.nil?)
           event={}
           row_values2.each {|column_family_and_column, value|
            column_family, column = column_family_and_column.split(":")
            if(column=="time")
              value=Time.at((value[0,10]).to_i).strftime("%Y%m%d%H%M%S")
            end
           (event[column_family.intern] ||= {}).update({column => value})
          }
           tbname=tbname.match(/[a-z]+(_[a-z]+|)/)
           tbname=tbname[0]+"_"+event[:info]["gid"]
           @newtable = MassiveRecord::Wrapper::Table.new(@conn, tbname.intern)
           rowid=(event[:info]["time"])[0,8]+"-"+event[:info]["dept"]+"-"+event[:info]["sid"]
           rowid=rowid+"-"+(event[:info]["time"])[8,6]+"-"+randnum(17)
           row = MassiveRecord::Wrapper::Row.new
           row.id = rowid
           row.values = event
           row.timestamp=(event[:info]["time"]).to_i
           row.table=@newtable
           row.save

         end

       }
       





    end
 end
end
