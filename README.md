# Kafka AccessLog to Metrics

## Nginx LogFormat

```
log_format json_combined escape=json
'{'
    '"body_bytes_sent":"$body_bytes_sent",'
    '"hostname":"$host",'
    '"http_referrer":"$http_referer",'
    '"http_user_agent":"$http_user_agent",'
    '"http_x_forwarded_for":"$http_x_forwarded_for",'
    '"remote_addr":"$remote_addr",'
    '"remote_user":"$remote_user",'
    '"request_method":"$request_method",'
    '"request_time":"$request_time",'
    '"request_uri":"$request_uri",'
    '"request":"$request",'
    '"scheme":"$scheme",'
    '"server_protocol":"$server_protocol",'
    '"status": "$status",'
    '"time_iso8601":"$time_iso8601",'    
    '"time_local":"$time_local",'
    '"upstream_connect_time":"$upstream_connect_time",'
    '"upstream_response_time":"$upstream_response_time",'
    '"url":"$scheme://$host$request_uri"'
'}';
```

```
access_log /var/log/nginx/access.json json_combined;
```

## Filebeat Config

```
filebeat.prospectors:
- paths:
   - /var/log/nginx/*.json
  input_type: log
  json.keys_under_root: true
  json.add_error_key: true

output.kafka:
  hosts: ["kafka:9092"]
  topic: 'filebeat'
  partition.round_robin:
    reachable_only: false
  required_acks: 1
  compression: gzip
  max_message_bytes: 1000000
```
