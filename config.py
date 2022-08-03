import base64

# 服务器列表
server_list = [
    'http://10.20.32.156:9090'
]

mysql_ip = '10.20.32.156'
redis_ip = '10.20.32.156'

# Prometheus Basic Http认证 Token

tokens = 'idss:letmein'  # Basic Http认证，格式为 账户:密码
tokens = 'Basic ' + str(base64.b64encode(tokens.encode('utf-8')), 'utf-8')
header = {
    "Authorization": tokens
}


# 查询清单

# ES指标清单
# 集群指标：
es_health_query = 'elasticsearch_cluster_health_status == 1'  # 集群健康值，query_info_static
es_pending_tasks_query = 'elasticsearch_cluster_health_number_of_pending_tasks'  # 任务积压 query_value_int
es_active_shards_query = 'elasticsearch_cluster_health_active_shards'  # 集群活跃分片总数  query_value_int
es_primary_shards_query = 'elasticsearch_cluster_health_active_primary_shards'  # 集群主分片数  query_value_int
es_init_shards_query = 'elasticsearch_cluster_health_initializing_shards'  # 正在初始化分片数  query_value_int
es_unass_shards_query = 'elasticsearch_cluster_health_unassigned_shards'  # 未分配分片数 query_value_int
es_delayed_shards_query = 'elasticsearch_cluster_health_delayed_unassigned_shards'  # 延迟分配分片数 query_value_int
es_relocating_shards_query = 'elasticsearch_cluster_health_relocating_shards'  # 重分配分片数 query_value_int
es_breakers_query = 'sum(elasticsearch_breakers_tripped) by (cluster)'  # 熔断器触发次数 query_value_int
es_qps_query = 'sum(rate(elasticsearch_indices_search_query_total [5m])) by (cluster)'  # 5分钟平均qps query_value_int

# ES节点指标:
es_node_ip_query = 'elasticsearch_nodes_roles'  # 节点ip地址 query_info_static
es_node_roles_query = 'elasticsearch_nodes_roles{role=~"master|data"}'  # 节点角色 query_info_static
es_node_cpu_query = 'elasticsearch_process_cpu_percent'  # 节点cpu使用率 query_value_int
es_node_heap_query = '(elasticsearch_jvm_memory_used_bytes/elasticsearch_jvm_memory_max_bytes) * 100'  # 节点堆内存使用率 query_value_int
es_node_disk_query = '(1-(elasticsearch_filesystem_data_available_bytes/elasticsearch_filesystem_data_size_bytes)) * 100'  # 磁盘使用率 query_value_int
es_node_fd_query = '(elasticsearch_process_open_files_count/elasticsearch_process_max_files_descriptors) * 100'  # 节点文件描述符使用率 query_value_int
es_node_ygc_query = 'elasticsearch_jvm_gc_collection_seconds_count{gc="young"}'  # 节点young gc次数 query_value_int
es_node_ygc_time_query = 'elasticsearch_jvm_gc_collection_seconds_sum{gc="young"}'  # 节点young gc 耗时  query_value_int
es_node_ogc_query = 'elasticsearch_jvm_gc_collection_seconds_count{gc="old"}'  # 节点old gc次数 query_value_int
es_node_ogc_time_query = 'elasticsearch_jvm_gc_collection_seconds_sum{gc="old"}'  # 节点old gc 耗时  query_value_int
es_node_bulk_reject_query = 'elasticsearch_thread_pool_rejected_count{type="write"}'  # 节点写失败计数 query_value_int
es_node_search_reject_query = 'elasticsearch_thread_pool_rejected_count{type="search"}'  # 节点查询失败计数 query_value_int
es_node_shards_query = 'elasticsearch_node_shards_total'  # 节点分片计数 query_value_int
es_node_qps_query = 'sum(rate(elasticsearch_indices_search_query_total [5m])) by (name)'  # 节点5分钟平均qps计数 query_value_int
es_node_iops_query = 'sum(rate(elasticsearch_filesystem_io_stats_device_operations_count [5m])) by (name)'  # 节点5分钟平均IOPS
es_node_indices_size_query = 'elasticsearch_indices_completion_size_in_bytes'  # es节点数据量
es_node_indices_docs_query = 'elasticsearch_indices_docs'  # es节点总文档数量

# kafka集群指标
kafka_brokers_query = 'kafka_brokers'
kafka_lag_query = 'sum(kafka_consumergroup_lag_sum) by (instance)'
kafka_topic_query = 'count(kafka_topic_partitions) by (instance)'
kafka_partitions_query = 'sum(kafka_topic_partitions{topic!="__consumer_offsets"}) by (instance)'
kafka_replicas_query = 'sum(kafka_topic_partition_replicas{topic!="__consumer_offsets"}) by (instance)'
kafka_sync_query = 'sum(kafka_topic_partition_in_sync_replica{topic!="__consumer_offsets"}) by (instance)'
kafka_under_replicated_query = 'sum(kafka_topic_partition_under_replicated_partition) by (instance)'
kafka_mps_query = 'sum(rate(kafka_topic_partition_current_offset [5m])) by (instance)'

# kafka消费组指标
kafka_consumer_lag_query = 'sum(kafka_consumergroup_lag >0) by (consumergroup, topic, partition)'

# zookeeper指标
zk_up_query = 'zk_up'
zk_fd_query = '(zk_open_file_descriptor_count / zk_max_file_descriptor_count) * 100'
zk_fd_used_query = 'zk_open_file_descriptor_count'
zk_lag_request_query = 'zk_outstanding_requests'
zk_pending_query = 'zk_pending_syncs'
zk_latency_query = 'avg(zk_avg_latency) by (zk_host)'
zk_send_packets_query = 'zk_packets_sent'

# mysql指标
my_status_query = 'mysql_global_status_uptime  >= bool 1'
my_up_query = 'mysql_global_status_uptime'  # 返回秒数
my_qps_query = 'rate(mysql_global_status_queries [5m]) or irate(mysql_global_status_queries [5m])'
my_conn_query = '(mysql_global_status_threads_connected / mysql_global_variables_max_connections) * 100'
my_fd_query = '(mysql_global_status_open_files / mysql_global_variables_open_files_limit) * 100'
my_fd_used_query = 'mysql_global_status_open_files'
my_slow_query = 'irate(mysql_global_status_slow_queries [24h])'  # mysql 最近24小时慢查询数量
my_tl_query = 'irate(mysql_global_status_table_locks_immediate [5m])'  # mysql 最近5分钟表锁数量

# redis 指标
redis_status_query = 'redis_uptime_in_seconds >= bool 1'
redis_up_query = 'redis_uptime_in_seconds'
redis_conn_query = 'redis_connected_clients'
redis_cps_query = 'rate(redis_commands_processed_total [5m])'  # 最近5分钟平均每秒命令执行
redis_hit_query = 'irate(redis_keyspace_hits_total [5m])'  # 最近5分钟平均每秒命中
redis_miss_query = 'irate(redis_keyspace_misses_total [5m])'  # 最近5分钟平均每秒miss
redis_mem_use_query = 'redis_memory_used_bytes'  # 单位 Bytes
redis_input_query = 'rate(redis_net_input_bytes_total [5m])'
redis_output_query = 'rate(redis_net_output_bytes_total [5m])'