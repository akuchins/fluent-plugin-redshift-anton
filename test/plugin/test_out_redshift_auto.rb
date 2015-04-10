require 'test_helper'

require 'fluent/test'
require 'fluent/plugin/out_redshift_auto'
require 'flexmock/test_unit'
require 'zlib'


class RedshiftOutputTest < Test::Unit::TestCase
  def setup
    require 'aws-sdk'
    require 'pg'
    require 'csv'
    Fluent::Test.setup
  end

  CONFIG_BASE= %[
    aws_key_id test_key_id
    aws_sec_key test_sec_key
    s3_bucket test_bucket
    path log
    redshift_host test_host
    redshift_dbname test_db
    redshift_user test_user
    redshift_password test_password
    redshift_tablename test_table
    buffer_type memory
    utc
    log_suffix id:5 host:localhost
  ]
  CONFIG_CSV= %[
    #{CONFIG_BASE}
    file_type csv
  ]
  CONFIG_TSV= %[
    #{CONFIG_BASE}
    file_type tsv
  ]
  CONFIG_JSON = %[
    #{CONFIG_BASE}
    file_type json
  ]
  CONFIG_JSON_WITH_SCHEMA = %[
    #{CONFIG_BASE}
    redshift_schemaname test_schema
    file_type json
  ]
  CONFIG_MSGPACK = %[
    #{CONFIG_BASE}
    file_type msgpack
  ]
  CONFIG_PIPE_DELIMITER= %[
    #{CONFIG_BASE}
    delimiter |
  ]
  CONFIG_PIPE_DELIMITER_WITH_NAME= %[
    #{CONFIG_BASE}
    file_type pipe
    delimiter |
  ]
  CONFIG=CONFIG_CSV

  RECORD_CSV_A = {"log" => %[val_a,val_b,val_c,val_d]}
  RECORD_CSV_B = {"log" => %[val_e,val_f,val_g,val_h]}
  RECORD_TSV_A = {"log" => %[val_a\tval_b\tval_c\tval_d]}
  RECORD_TSV_B = {"log" => %[val_e\tval_f\tval_g\tval_h]}
  RECORD_JSON_A = {"log" => %[{"key_a" : "val_a", "key_b" : "val_b"}]}
  RECORD_JSON_B = {"log" => %[{"key_c" : "val_c", "key_d" : "val_d"}]}
  RECORD_MSGPACK_A = {"key_a" => "val_a", "key_b" => "val_b"}
  RECORD_MSGPACK_B = {"key_c" => "val_c", "key_d" => "val_d"}
  DEFAULT_TIME = Time.parse("2013-03-06 12:15:02 UTC").to_i

  def create_driver(conf = CONFIG, tag='test.input')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedshiftOutput, tag).configure(conf)
  end

  def create_driver_no_write(conf = CONFIG, tag='test.input')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedshiftOutput, tag) do
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver(CONFIG_BASE)
    }
    d = create_driver(CONFIG_CSV)
    assert_equal "test_key_id", d.instance.aws_key_id
    assert_equal "test_sec_key", d.instance.aws_sec_key
    assert_equal "test_bucket", d.instance.s3_bucket
    assert_equal "log/", d.instance.path
    assert_equal "test_host", d.instance.redshift_host
    assert_equal 5439, d.instance.redshift_port
    assert_equal "test_db", d.instance.redshift_dbname
    assert_equal "test_user", d.instance.redshift_user
    assert_equal "test_password", d.instance.redshift_password
    assert_equal "test_table", d.instance.redshift_tablename
    assert_equal nil, d.instance.redshift_schemaname
    assert_equal "FILLRECORD ACCEPTANYDATE TRUNCATECOLUMNS", d.instance.redshift_copy_base_options
    assert_equal nil, d.instance.redshift_copy_options
    assert_equal "csv", d.instance.file_type
    assert_equal ",", d.instance.delimiter
    assert_equal true, d.instance.utc
  end
  def test_configure_with_schemaname
    d = create_driver(CONFIG_JSON_WITH_SCHEMA)
    assert_equal "test_schema", d.instance.redshift_schemaname
  end
  def test_configure_localtime
    d = create_driver(CONFIG_CSV.gsub(/ *utc */, ''))
    assert_equal false, d.instance.utc
  end
  def test_configure_no_path
    d = create_driver(CONFIG_CSV.gsub(/ *path *.+$/, ''))
    assert_equal "", d.instance.path
  end
  def test_configure_root_path
    d = create_driver(CONFIG_CSV.gsub(/ *path *.+$/, 'path /'))
    assert_equal "", d.instance.path
  end
  def test_configure_path_with_slash
    d = create_driver(CONFIG_CSV.gsub(/ *path *.+$/, 'path log/'))
    assert_equal "log/", d.instance.path
  end
  def test_configure_path_starts_with_slash
    d = create_driver(CONFIG_CSV.gsub(/ *path *.+$/, 'path /log/'))
    assert_equal "log/", d.instance.path
  end
  def test_configure_path_starts_with_slash_without_last_slash
    d = create_driver(CONFIG_CSV.gsub(/ *path *.+$/, 'path /log'))
    assert_equal "log/", d.instance.path
  end
  def test_configure_tsv
    d1 = create_driver(CONFIG_TSV)
    assert_equal "tsv", d1.instance.file_type
    assert_equal "\t", d1.instance.delimiter
  end
  def test_configure_json
    d2 = create_driver(CONFIG_JSON)
    assert_equal "json", d2.instance.file_type
    assert_equal "\t", d2.instance.delimiter
  end
  def test_configure_msgpack
    d2 = create_driver(CONFIG_MSGPACK)
    assert_equal "msgpack", d2.instance.file_type
    assert_equal "\t", d2.instance.delimiter
  end
  def test_configure_original_file_type
    d3 = create_driver(CONFIG_PIPE_DELIMITER)
    assert_equal nil, d3.instance.file_type
    assert_equal "|", d3.instance.delimiter

    d4 = create_driver(CONFIG_PIPE_DELIMITER_WITH_NAME)
    assert_equal "pipe", d4.instance.file_type
    assert_equal "|", d4.instance.delimiter
  end
  def test_configure_no_log_suffix
    d = create_driver(CONFIG_CSV.gsub(/ *log_suffix *.+$/, ''))
    assert_equal "", d.instance.log_suffix
  end

  def emit_csv(d)
    d.emit(RECORD_CSV_A, DEFAULT_TIME)
    d.emit(RECORD_CSV_B, DEFAULT_TIME)
  end
  def emit_tsv(d)
    d.emit(RECORD_TSV_A, DEFAULT_TIME)
    d.emit(RECORD_TSV_B, DEFAULT_TIME)
  end
  def emit_json(d)
    d.emit(RECORD_JSON_A, DEFAULT_TIME)
    d.emit(RECORD_JSON_B, DEFAULT_TIME)
  end
  def emit_msgpack(d)
    d.emit(RECORD_MSGPACK_A, DEFAULT_TIME)
    d.emit(RECORD_MSGPACK_B, DEFAULT_TIME)
  end

  def test_format_csv
    d_csv = create_driver_no_write(CONFIG_CSV)
    emit_csv(d_csv)
    d_csv.expect_format RECORD_CSV_A['log'] + "\n"
    d_csv.expect_format RECORD_CSV_B['log'] + "\n"
    d_csv.run
  end
  def test_format_tsv
    d_tsv = create_driver_no_write(CONFIG_TSV)
    emit_tsv(d_tsv)
    d_tsv.expect_format RECORD_TSV_A['log'] + "\n"
    d_tsv.expect_format RECORD_TSV_B['log'] + "\n"
    d_tsv.run
  end
  def test_format_json
    d_json = create_driver_no_write(CONFIG_JSON)
    emit_json(d_json)
    d_json.expect_format RECORD_JSON_A.to_msgpack
    d_json.expect_format RECORD_JSON_B.to_msgpack
    d_json.run
  end

  def test_format_msgpack
    d_msgpack = create_driver_no_write(CONFIG_MSGPACK)
    emit_msgpack(d_msgpack)
    d_msgpack.expect_format({ 'log' => RECORD_MSGPACK_A }.to_msgpack)
    d_msgpack.expect_format({ 'log' => RECORD_MSGPACK_B }.to_msgpack)
    d_msgpack.run
  end

  class PGConnectionMock
    def initialize(options = {})
      @return_keys = options[:return_keys] || ['key_a', 'key_b', 'key_c', 'key_d', 'key_e', 'key_f', 'key_g', 'key_h']
      @target_schema = options[:schemaname] || nil
      @target_table = options[:tablename] || 'test_table'
    end

    def expected_column_list_query
      if @target_schema
        /\Aselect column_name from INFORMATION_SCHEMA.COLUMNS where table_schema = '#{@target_schema}' and table_name = '#{@target_table}'/
      else
        /\Aselect column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '#{@target_table}'/
      end
    end

    def expected_copy_query
      if @target_schema
        /\Acopy #{@target_schema}.#{@target_table} from/
      else
        /\Acopy #{@target_table} from/
      end
    end

    def exec(sql, &block)
      if block_given?
        if sql =~ expected_column_list_query
          yield @return_keys.collect{|key| {'column_name' => key}}
        else
          yield []
        end
      else
        unless sql =~ expected_copy_query
          error = PG::Error.new("ERROR:  Load into table '#{@target_table}' failed.  Check 'stl_load_errors' system table for details.")
          error.result = "ERROR:  Load into table '#{@target_table}' failed.  Check 'stl_load_errors' system table for details."
          raise error
        end
      end
    end

    def close
    end
  end

  def setup_pg_mock
    # create mock of PG
    def PG.connect(dbinfo)
      return PGConnectionMock.new
    end
  end

  def setup_s3_mock(expected_data)
    current_time = Time.now

    # create mock of s3 object
    s3obj = flexmock(AWS::S3::S3Object)
    s3obj.should_receive(:exists?).with_any_args.and_return { false }
    s3obj.should_receive(:write).with(
      # pathname
      on { |pathname|
        data = nil
        pathname.open { |f|
          gz = Zlib::GzipReader.new(f)
          data = gz.read
          gz.close
        }
        assert_equal expected_data, data
      },
      :acl => :bucket_owner_full_control
    ).and_return { true }

    # create mock of s3 object collection
    s3obj_col = flexmock(AWS::S3::ObjectCollection)
    s3obj_col.should_receive(:[]).with(
      on { |key|
        expected_key = current_time.utc.strftime("log/year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M_00.gz")
        key == expected_key
      }).
      and_return {
        s3obj
      }

    # create mock of s3 bucket
    flexmock(AWS::S3::Bucket).new_instances do |bucket|
      bucket.should_receive(:objects).with_any_args.
        and_return {
          s3obj_col
        }
    end
  end

  def setup_tempfile_mock_to_be_closed
    flexmock(Tempfile).new_instances.should_receive(:close!).at_least.once
  end

  def setup_mocks(expected_data)
    setup_pg_mock
    setup_s3_mock(expected_data) end

  def test_write_with_csv
    setup_mocks(%[val_a,val_b,val_c,val_d\nval_e,val_f,val_g,val_h\n])
    setup_tempfile_mock_to_be_closed
    d_csv = create_driver
    emit_csv(d_csv)
    assert_equal true, d_csv.run
  end

  def test_write_with_json
    setup_mocks(%[val_a\tval_b\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n])
    setup_tempfile_mock_to_be_closed
    d_json = create_driver(CONFIG_JSON)
    emit_json(d_json)
    assert_equal true, d_json.run
  end

  def test_write_with_json_hash_value
    setup_mocks("val_a\t{\"foo\":\"var\"}\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_json = create_driver(CONFIG_JSON)
    d_json.emit({"log" => %[{"key_a" : "val_a", "key_b" : {"foo" : "var"}}]} , DEFAULT_TIME)
    d_json.emit(RECORD_JSON_B, DEFAULT_TIME)
    assert_equal true, d_json.run
  end

  def test_write_with_json_array_value
    setup_mocks("val_a\t[\"foo\",\"var\"]\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_json = create_driver(CONFIG_JSON)
    d_json.emit({"log" => %[{"key_a" : "val_a", "key_b" : ["foo", "var"]}]} , DEFAULT_TIME)
    d_json.emit(RECORD_JSON_B, DEFAULT_TIME)
    assert_equal true, d_json.run
  end

  def test_write_with_json_including_tab_newline_quote
    setup_mocks("val_a_with_\\\t_tab_\\\n_newline\tval_b_with_\\\\_quote\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_json = create_driver(CONFIG_JSON)
    d_json.emit({"log" => %[{"key_a" : "val_a_with_\\t_tab_\\n_newline", "key_b" : "val_b_with_\\\\_quote"}]} , DEFAULT_TIME)
    d_json.emit(RECORD_JSON_B, DEFAULT_TIME)
    assert_equal true, d_json.run
  end

  def test_write_with_json_no_data
    setup_mocks("")
    d_json = create_driver(CONFIG_JSON)
    d_json.emit("", DEFAULT_TIME)
    d_json.emit("", DEFAULT_TIME)
    assert_equal false, d_json.run
  end

  def test_write_with_json_invalid_one_line
    setup_mocks(%[\t\tval_c\tval_d\t\t\t\t\n])
    d_json = create_driver(CONFIG_JSON)
    d_json.emit({"log" => %[}}]}, DEFAULT_TIME)
    d_json.emit(RECORD_JSON_B, DEFAULT_TIME)
    assert_equal true, d_json.run
  end

  def test_write_with_json_no_available_data
    setup_mocks(%[val_a\tval_b\t\t\t\t\t\t\n])
    d_json = create_driver(CONFIG_JSON)
    d_json.emit(RECORD_JSON_A, DEFAULT_TIME)
    d_json.emit({"log" => %[{"key_o" : "val_o", "key_p" : "val_p"}]}, DEFAULT_TIME)
    assert_equal true, d_json.run
  end

  def test_write_with_msgpack
    setup_mocks(%[val_a\tval_b\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n])
    d_msgpack = create_driver(CONFIG_MSGPACK)
    emit_msgpack(d_msgpack)
    assert_equal true, d_msgpack.run
  end

  def test_write_with_msgpack_hash_value
    setup_mocks("val_a\t{\"foo\":\"var\"}\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_msgpack = create_driver(CONFIG_MSGPACK)
    d_msgpack.emit({"key_a" => "val_a", "key_b" => {"foo" => "var"}} , DEFAULT_TIME)
    d_msgpack.emit(RECORD_MSGPACK_B, DEFAULT_TIME)
    assert_equal true, d_msgpack.run
  end

  def test_write_with_msgpack_array_value
    setup_mocks("val_a\t[\"foo\",\"var\"]\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_msgpack = create_driver(CONFIG_MSGPACK)
    d_msgpack.emit({"key_a" => "val_a", "key_b" => ["foo", "var"]} , DEFAULT_TIME)
    d_msgpack.emit(RECORD_MSGPACK_B, DEFAULT_TIME)
    assert_equal true, d_msgpack.run
  end

  def test_write_with_msgpack_including_tab_newline_quote
    setup_mocks("val_a_with_\\\t_tab_\\\n_newline\tval_b_with_\\\\_quote\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n")
    d_msgpack = create_driver(CONFIG_MSGPACK)
    d_msgpack.emit({"key_a" => "val_a_with_\t_tab_\n_newline", "key_b" => "val_b_with_\\_quote"} , DEFAULT_TIME)
    d_msgpack.emit(RECORD_MSGPACK_B, DEFAULT_TIME)
    assert_equal true, d_msgpack.run
  end

  def test_write_with_msgpack_no_data
    setup_mocks("")
    d_msgpack = create_driver(CONFIG_MSGPACK)
    d_msgpack.emit({}, DEFAULT_TIME)
    d_msgpack.emit({}, DEFAULT_TIME)
    assert_equal false, d_msgpack.run
  end

  def test_write_with_msgpack_no_available_data
    setup_mocks(%[val_a\tval_b\t\t\t\t\t\t\n])
    d_msgpack = create_driver(CONFIG_MSGPACK)
    d_msgpack.emit(RECORD_MSGPACK_A, DEFAULT_TIME)
    d_msgpack.emit({"key_o" => "val_o", "key_p" => "val_p"}, DEFAULT_TIME)
    assert_equal true, d_msgpack.run
  end

  def test_write_redshift_connection_error
    def PG.connect(dbinfo)
      return Class.new do
        def initialize(return_keys=[]); end
        def exec(sql)
          raise PG::Error, "redshift connection error"
        end
        def close; end
      end.new
    end
    setup_s3_mock(%[val_a,val_b,val_c,val_d\nval_e,val_f,val_g,val_h\n])

    d_csv = create_driver
    emit_csv(d_csv)
    assert_raise(PG::Error) {
      d_csv.run
    }
  end

  def test_write_redshift_load_error
    PG::Error.module_eval { attr_accessor :result}
    def PG.connect(dbinfo)
      return Class.new do
        def initialize(return_keys=[]); end
        def exec(sql)
          error = PG::Error.new("ERROR:  Load into table 'apache_log' failed.  Check 'stl_load_errors' system table for details.")
          error.result = "ERROR:  Load into table 'apache_log' failed.  Check 'stl_load_errors' system table for details."
          raise error
        end
        def close; end
      end.new
    end
    setup_s3_mock(%[val_a,val_b,val_c,val_d\nval_e,val_f,val_g,val_h\n])

    d_csv = create_driver
    emit_csv(d_csv)
    assert_equal false,  d_csv.run
  end

  def test_write_with_json_redshift_connection_error
    def PG.connect(dbinfo)
      return Class.new do
        def initialize(return_keys=[]); end
        def exec(sql, &block)
          error = PG::Error.new("redshift connection error")
          raise error
        end
        def close; end
      end.new
    end
    setup_s3_mock(%[val_a,val_b,val_c,val_d\nval_e,val_f,val_g,val_h\n])

    d_json = create_driver(CONFIG_JSON)
    emit_json(d_json)
    assert_raise(PG::Error) {
      d_json.run
    }
  end

  def test_write_with_json_no_table_on_redshift
    def PG.connect(dbinfo)
      return Class.new do
        def initialize(return_keys=[]); end
        def exec(sql, &block)
          yield [] if block_given?
        end
        def close; end
      end.new
    end
    setup_s3_mock(%[val_a,val_b,val_c,val_d\nval_e,val_f,val_g,val_h\n])

    d_json = create_driver(CONFIG_JSON)
    emit_json(d_json)
    assert_equal false, d_json.run
  end

  def test_write_with_json_failed_to_get_columns
    def PG.connect(dbinfo)
      return Class.new do
        def initialize(return_keys=[]); end
        def exec(sql, &block)
        end
        def close; end
      end.new
    end
    setup_s3_mock("")

    d_json = create_driver(CONFIG_JSON)
    emit_json(d_json)
    assert_raise(RuntimeError, "failed to fetch the redshift table definition.") {
      d_json.run
    }
  end

  def test_write_with_json_fetch_column_with_schema
    def PG.connect(dbinfo)
      return PGConnectionMock.new(:schemaname => 'test_schema')
    end
    setup_s3_mock(%[val_a\tval_b\t\t\t\t\t\t\n\t\tval_c\tval_d\t\t\t\t\n])
    d_json = create_driver(CONFIG_JSON_WITH_SCHEMA)
    emit_json(d_json)
    assert_equal true, d_json.run
  end
end
