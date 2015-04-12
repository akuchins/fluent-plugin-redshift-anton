Amazon Redshift output plugin for Fluentd
========

## Overview

Amazon Redshift output plugin uploads event logs to an Amazon Redshift Cluster. Supportted data formats are csv, tsv and json. An S3 bucket and a Redshift Cluster are required to use this plugin.

## Installation

    gem install fluent-plugin-redshift-anton

## Configuration

Format:

    <match my.tag>
        type redshift_anton

        # s3 (for copying data to redshift)
        aws_key_id YOUR_AWS_KEY_ID
        aws_sec_key YOUR_AWS_SECRET_KEY
        s3_bucket YOUR_S3_BUCKET
        s3_endpoint YOUR_S3_BUCKET_END_POINT
        path YOUR_S3_PATH
        timestamp_key_format year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M

        # redshift
        redshift_host YOUR_AMAZON_REDSHIFT_CLUSTER_END_POINT
        redshift_port YOUR_AMAZON_REDSHIFT_CLUSTER_PORT
        redshift_dbname YOUR_AMAZON_REDSHIFT_CLUSTER_DATABASE_NAME
        redshift_user YOUR_AMAZON_REDSHIFT_CLUSTER_USER_NAME
        redshift_password YOUR_AMAZON_REDSHIFT_CLUSTER_PASSWORD
        redshift_schemaname YOUR_AMAZON_REDSHIFT_CLUSTER_TARGET_SCHEMA_NAME
        redshift_tablename YOUR_AMAZON_REDSHIFT_CLUSTER_TARGET_TABLE_NAME
        redshift_copy_base_options YOUR_REDSHIFT_COPY_CUSTOM_PARAMETERS
        make_auto_table 1 # 1 => make table auto 0 => no
        tag_table 1 # 1=> tag_name = table_name, 0 => no, use redshift_atablename
        file_type [tsv|csv|json|msgpack]
        delimiter nil # derived from file_type: \t for tsv, json, ',' for csv
        varchar_length ALL_COLUMNS_VARCHAR_LENGTH


        # buffer
        buffer_type file
        buffer_path /var/log/fluent/redshift
        flush_interval 15m
        buffer_chunk_limit 1g
    </match>

Example (watch and upload json formatted apache log):

    <source>
        type tail
        path redshift_test.json
        pos_file redshift_test_json.pos
        tag redshift.json
        format /^(?<log>.*)$/
    </source>

    <match redshift.json>
        type redshift

        # s3 (for copying data to redshift)
        aws_key_id YOUR_AWS_KEY_ID
        aws_sec_key YOUR_AWS_SECRET_KEY
        s3_bucket hapyrus-example
        s3_endpoint s3.amazonaws.com
        path path/on/s3/apache_json_log/
        timestamp_key_format year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M

        # redshift
        redshift_host xxx-yyy-zzz.xxxxxxxxxx.us-east-1.redshift.amazonaws.com
        redshift_port 5439
        redshift_dbname fluent-redshift-test
        redshift_user fluent
        redshift_password fluent-password
        redshift_tablename apache_log
        file_type json

        # buffer
        buffer_type file
        buffer_path /var/log/fluent/redshift
        flush_interval 15m
        buffer_chunk_limit 1g
    <match>

+ `type` (required) : The value must be `redshift`.

+ `aws_key_id` (required) : AWS access key id to access s3 bucket.

+ `aws_sec_key` (required) : AWS securet key id to access s3 bucket.

+ `s3_bucket` (required) : s3 bucket name. S3 bucket must be same as the region of your Redshift cluster.

+ `s3_endpoint` : s3 endpoint.

+ `path` (required) : s3 path to input.

+ `timestamp_key_format` : The format of the object keys. It can include date-format directives.

  - Default parameter is "year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M"
  - For example, the s3 path is as following with the above example configration.
    <pre>
  hapyrus-example/apache_json_log/year=2013/month=03/day=05/hour=12/20130305_1215_00.gz
  hapyrus-example/apache_json_log/year=2013/month=03/day=05/hour=12/20130305_1230_00.gz
</pre>

+ `redshift_host` (required) : the end point(or hostname) of your Amazon Redshift cluster.

+ `redshift_port` (required) : port number.

+ `redshift_dbname` (required) : database name.

+ `redshift_user` (required) : user name.

+ `redshift_password` (required) : password for the user name.

+ `redshift_tablename` (required) : table name to store data.

+ `redshift_schemaname` : schema name to store data. By default, this option is not set and find table without schema as your own search_path.

+ `redshift_copy_base_options` : additional options to pass the Redshift COPY command, such as TIMEFORMAT.

+ `make_auto_table` (optional, integer) : whether make tables automatically. If you set 1, this makes tables automatically else if you set 0, doesn't make.

+ `tag_table` (optional, integer) : whether table_name equals tag_name. If you set 1, it shows tag_name equals table_name, else if you set 0, it's not.

+ `file_type` : file format of the source data.  `csv`, `tsv`, `msgpack` or `json` are available.

+ `delimiter` : delimiter of the source data. This option will be automatically set if `file_type` is specified.

+ `buffer_type` : buffer type.

+ `buffer_path` : path prefix of the files to buffer logs.

+ `flush_interval` : flush interval.

+ `buffer_chunk_limit` : limit buffer size to chunk.

+ `utc` : utc time zone. This parameter affects `timestamp_key_format`.

## Logging examples
```ruby
# examples by fluent-logger
require 'fluent-logger'
log = Fluent::Logger::FluentLogger.new(nil, :host => 'localhost', :port => 24224)

# file_type: csv
log.post('your.tag', :log => "12345,12345")

# file_type: tsv
log.post('your.tag', :log => "12345\t12345")

# file_type: json
require 'json'
log.post('your.tag', :log => { :user_id => 12345, :data_id => 12345 }.to_json)

# file_type: msgpack
log.post('your.tag', :user_id => 12345, :data_id => 12345)
```

## License

Copyright (c) 2013 [Hapyrus Inc](http://hapyrus.com)

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
