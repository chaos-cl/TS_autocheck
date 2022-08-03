# -*- coding:utf-8 -*-
"""
2022.07.19 cuilei@idss-cn.com; 拉取Prometheus接口指标，并做相应计算，输出巡检指标并写入excel。
"""
import PrometheusApi
import PrometheusHostApi
import config
from config import *


def es_check(filename, auth='Y'):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusApi.PrometheusApi(url=url, headers=headers)
        print('====================获取es集群指标====================')
        es_healthy = client.query_info_static(querystr=es_health_query, keywords='cluster', valuekey='color')
        es_pending_tasks = client.query_value_int(querystr=es_pending_tasks_query, keywords='cluster')
        es_active_shards = client.query_value_int(querystr=es_active_shards_query, keywords='cluster')
        es_primary_shards = client.query_value_int(querystr=es_primary_shards_query, keywords='cluster')
        es_init_shards = client.query_value_int(querystr=es_init_shards_query, keywords='cluster')
        es_unassigned_shards = client.query_value_int(querystr=es_unass_shards_query, keywords='cluster')
        es_delayed_shards = client.query_value_int(querystr=es_delayed_shards_query, keywords='cluster')
        es_relocating_shards = client.query_value_int(querystr=es_relocating_shards_query, keywords='cluster')
        es_breakers_tripped = client.query_value_int(querystr=es_breakers_query, keywords='cluster')
        es_cluster_qps = client.query_value_percent(querystr=es_qps_query, keywords='cluster', ispercent='N')
        print('==================es集群指标采集完毕===================')
        print('====================获取es节点指标====================')
        es_node_ip = client.query_info_static(querystr=es_node_ip_query, keywords='name', valuekey='host')
        es_node_roles = client.query_info_static(querystr=es_node_roles_query, keywords='name', valuekey='role')
        es_node_cpu = client.query_value_percent(querystr=es_node_cpu_query, keywords='name')
        es_node_heap = client.query_value_percent(querystr=es_node_heap_query, keywords='name')
        es_node_disk = client.query_value_percent(querystr=es_node_disk_query, keywords='name')
        es_node_fd = client.query_value_percent(querystr=es_node_fd_query, keywords='name')
        es_node_ygc = client.query_value_int(querystr=es_node_ygc_query, keywords='name')
        es_node_ygc_time = client.query_value_int(querystr=es_node_ygc_time_query, keywords='name')
        es_node_ogc = client.query_value_int(querystr=es_node_ogc_query, keywords='name')
        es_node_ogc_time = client.query_value_int(querystr=es_node_ogc_time_query, keywords='name')
        es_node_bulk_reject = client.query_value_int(querystr=es_node_bulk_reject_query, keywords='name')
        es_node_search_reject = client.query_value_int(querystr=es_node_search_reject_query, keywords='name')
        es_node_shards = client.query_value_int(querystr=es_node_shards_query, keywords='node')
        es_node_qps = client.query_value_percent(querystr=es_node_qps_query, keywords='name', ispercent='N')
        es_node_iops = client.query_value_percent(querystr=es_node_iops_query, keywords='name', ispercent='N')
        es_node_indices_size = client.query_value_int(querystr=es_node_indices_size_query, keywords='name')
        es_node_indices_docs = client.query_value_int(querystr=es_node_indices_docs_query, keywords='name')
        print('===================es集群指标采集完毕==================')
        cluster_list = []
        cluster_data = []
        for cluster, values in es_healthy.items():
            if cluster not in cluster_list:
                cluster_list.append(cluster)
                cluster_data.append(
                    {
                        'cluster': cluster,
                        'healthy': es_healthy.get(cluster),
                        'pending_task': es_pending_tasks.get(cluster),
                        'total_shards': es_active_shards.get(cluster),
                        'primary_shards': es_primary_shards.get(cluster),
                        'init_shards': es_init_shards.get(cluster),
                        'unassign_shards': es_unassigned_shards.get(cluster),
                        'delayed_shards': es_delayed_shards.get(cluster),
                        'relocating_shards': es_relocating_shards.get(cluster),
                        'breaker_tripped': es_breakers_tripped.get(cluster),
                        'qps': es_cluster_qps.get(cluster)
                    }
                )
        node_list = []
        node_data = []
        for node, values in es_node_ip.items():
            if node not in node_list:
                node_list.append(node)
                node_data.append(
                    {
                        'es_node_name': node,
                        'es_node_ip': es_node_ip.get(node),
                        'es_node_roles': es_node_roles.get(node),
                        'es_node_cpu': es_node_cpu.get(node),
                        'es_node_heap': es_node_heap.get(node),
                        'es_node_disk': es_node_disk.get(node),
                        'es_node_fd': es_node_fd.get(node),
                        'es_node_ygc': es_node_ygc.get(node),
                        'es_node_ygc_time': es_node_ygc_time.get(node),
                        'es_node_ogc': es_node_ogc.get(node),
                        'es_node_ogc_time': es_node_ogc_time.get(node),
                        'es_node_bulk_reject': es_node_bulk_reject.get(node),
                        'es_node_search_reject': es_node_search_reject.get(node),
                        'es_node_shards': es_node_shards.get(node),
                        'es_node_qps': es_node_qps.get(node),
                        'es_node_iops': es_node_iops.get(node),
                        'es_node_indices_size': PrometheusApi.bytes_to_mb(es_node_indices_size.get(node)),
                        'es_node_indices_docs': es_node_indices_docs.get(node)
                    }
                )
        PrometheusApi.export_es_excel(cluster=cluster_data, node=node_data, filename=filename)
    print('===================es指标输出完成！===================')


def kafka_check(filename, auth='Y'):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusApi.PrometheusApi(url=url, headers=headers)
        print('====================获取kafka集群指标====================')
        kafka_brokers = client.query_value_int(querystr=kafka_brokers_query, keywords='instance')
        kafka_lag = client.query_value_int(querystr=kafka_lag_query, keywords='instance')
        kafka_topic = client.query_value_int(querystr=kafka_topic_query, keywords='instance')
        kafka_partitions = client.query_value_int(querystr=kafka_partitions_query, keywords='instance')
        kafka_replicas = client.query_value_int(querystr=kafka_replicas_query, keywords='instance')
        kafka_sync = client.query_value_int(querystr=kafka_sync_query, keywords='instance')
        kafka_under_replicated = client.query_value_int(querystr=kafka_under_replicated_query, keywords='instance')
        kafka_mps = client.query_value_percent(querystr=kafka_mps_query, keywords='instance', ispercent='N')
        print('==================kafka集群指标采集完毕===================')
        print('====================获取kafka消费组指标====================')
        kafka_consumer_lag = client.query_kafka_consumer(querystr=kafka_consumer_lag_query)
        print('===================kafka消费组指标采集完毕==================')
        cluster_list = []
        cluster_data = []
        for instance, values in kafka_brokers.items():
            if instance not in cluster_list:
                cluster_list.append(instance)
                cluster_data.append(
                    {
                        'kafka_brokers': kafka_brokers.get(instance),
                        'kafka_lag': kafka_lag.get(instance),
                        'kafka_topic': kafka_topic.get(instance),
                        'kafka_partitions': kafka_partitions.get(instance),
                        'kafka_replicas': kafka_replicas.get(instance),
                        'kafka_sync': kafka_sync.get(instance),
                        'kafka_under_replicated': kafka_under_replicated.get(instance),
                        'kafka_mps': kafka_mps.get(instance)
                    }
                )

        consumer_data = kafka_consumer_lag
        PrometheusApi.export_kafka_excel(cluster=cluster_data, consumer=consumer_data, filename=filename)
    print('===================kafka指标输出完成！===================')


def zookeeper_check(filename, auth='Y'):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusApi.PrometheusApi(url=url, headers=headers)
        print('====================获取zookeeper集群指标====================')
        zk_up = client.query_value_int(querystr=zk_up_query, keywords='zk_host')
        zk_fd = client.query_value_percent(querystr=zk_fd_query, keywords='zk_host')
        zk_fd_used = client.query_value_int(querystr=zk_fd_used_query, keywords='zk_host')
        zk_lag_request = client.query_value_int(querystr=zk_lag_request_query, keywords='zk_host')
        zk_pending = client.query_value_int(querystr=zk_pending_query, keywords='zk_host')
        zk_latency = client.query_value_percent(querystr=zk_latency_query, keywords='zk_host', ispercent='N')
        zk_send_packets = client.query_value_int(querystr=zk_send_packets_query, keywords='zk_host')
        print('==================zookeeper集群指标采集完毕===================')

        cluster_list = []
        cluster_data = []
        for instance, values in zk_up.items():
            if instance not in cluster_list:
                cluster_list.append(instance)
                if int(zk_up.get(instance)) == 1:
                    status = '正常'
                else:
                    status = '异常'
                cluster_data.append(
                    {
                        'zk_ip': instance,
                        'zk_up': status,
                        'zk_fd': zk_fd.get(instance),
                        'zk_fd_used': zk_fd_used.get(instance),
                        'zk_lag_request': zk_lag_request.get(instance),
                        'zk_pending': zk_pending.get(instance),
                        'zk_latency': zk_latency.get(instance),
                        'zk_send_packets': zk_send_packets.get(instance)
                    }
                )
        #print(cluster_data)
        PrometheusApi.export_zk_excel(cluster=cluster_data, filename=filename)
    print('===================zookeeper指标输出完成！===================')


def mysql_check(filename, auth='Y'):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusApi.PrometheusApi(url=url, headers=headers)
        print('====================获取mysql指标====================')
        my_status = client.query_value_int(querystr=my_status_query, keywords='instance', replace=':49256')
        my_up = client.query_value_int(querystr=my_up_query, keywords='instance', replace=':49256')
        my_qps = client.query_value_percent(querystr=my_qps_query, keywords='instance', replace=':49256', ispercent='N')
        my_conn = client.query_value_percent(querystr=my_conn_query, keywords='instance', replace=':49256', ispercent='Y')
        my_fd = client.query_value_percent(querystr=my_fd_query, keywords='instance', replace=':49256', ispercent='Y')
        my_fd_used = client.query_value_int(querystr=my_fd_used_query, keywords='instance', replace=':49256')
        my_slow = client.query_value_int(querystr=my_slow_query, keywords='instance', replace=':49256')
        my_tl = client.query_value_percent(querystr=my_tl_query, keywords='instance', replace=':49256', ispercent='N')
        print('==================mysql指标采集完毕===================')

        cluster_list = []
        cluster_data = []
        for instance, values in my_status.items():
            if instance not in cluster_list:
                cluster_list.append(instance)
                if int(my_status.get(instance)) == 1:
                    status = '正常'
                else:
                    status = '异常'
                cluster_data.append(
                    {
                        'my_ip': config.mysql_ip,
                        'my_status': status,
                        'my_up': PrometheusApi.s_to_d(my_up.get(instance)),
                        'my_qps': my_qps.get(instance),
                        'my_conn': my_conn.get(instance),
                        'my_fd': my_fd.get(instance),
                        'my_fd_used': my_fd_used.get(instance),
                        'my_slow': my_slow.get(instance),
                        'my_tl': my_tl.get(instance)
                    }
                )
        # print(cluster_data)
        PrometheusApi.export_my_excel(cluster=cluster_data, filename=filename)
    print('===================mysql指标输出完成！===================')


def redis_check(filename, auth='Y'):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusApi.PrometheusApi(url=url, headers=headers)
        print('====================获取redis指标====================')
        redis_status = client.query_value_int(querystr=redis_status_query, keywords='instance', replace=':49257')
        redis_up = client.query_value_int(querystr=redis_up_query, keywords='instance', replace=':49257')
        redis_conn = client.query_value_int(querystr=redis_conn_query, keywords='instance', replace=':49257')
        redis_cps = client.query_value_percent(querystr=redis_cps_query, keywords='instance', replace=':49257', ispercent='N')
        redis_hit = client.query_value_int(querystr=redis_hit_query, keywords='instance', replace=':49257')
        redis_miss = client.query_value_int(querystr=redis_miss_query, keywords='instance', replace=':49257')
        redis_mem_use = client.query_value_int(querystr=redis_mem_use_query, keywords='instance', replace=':49257')
        redis_input = client.query_value_int(querystr=redis_input_query, keywords='instance', replace=':49257')
        redis_output = client.query_value_int(querystr=redis_output_query, keywords='instance', replace=':49257')
        print('==================redis指标采集完毕===================')

        cluster_list = []
        cluster_data = []
        for instance, values in redis_status.items():
            if instance not in cluster_list:
                cluster_list.append(instance)
                if int(redis_status.get(instance)) == 1:
                    status = '正常'
                else:
                    status = '异常'
                cluster_data.append(
                    {
                        'redis_ip': config.redis_ip,
                        'redis_status': status,
                        'redis_up': PrometheusApi.s_to_d(redis_up.get(instance)),
                        'redis_conn': redis_conn.get(instance),
                        'redis_cps': redis_cps.get(instance),
                        'redis_hit': redis_hit.get(instance),
                        'redis_miss': redis_miss.get(instance),
                        'redis_mem_use': PrometheusApi.bytes_to_mb(redis_mem_use.get(instance)),
                        'redis_input': PrometheusApi.bytes_to_mb(redis_input.get(instance)),
                        'redis_output': PrometheusApi.bytes_to_mb(redis_output.get(instance))
                    }
                )
        # print(cluster_data)
        PrometheusApi.export_redis_excel(cluster=cluster_data, filename=filename)
    print('===================redis指标输出完成！===================')


def host_check(filename, auth="Y"):
    if auth == 'Y':
        headers = config.header
    else:
        headers = {}
    serverlist = server_list
    for url in serverlist:
        client = PrometheusHostApi.PrometheusHostApi(url=url, headers=headers)
        hostname = client.get_hostname()
        print('===========完成主机名采集！===========')
        uptime = client.get_uptime()
        print('===========完成运行时间采集！===========')
        mem = client.get_mem()
        print('===========完成内存大小采集!===========')
        cpu_cores = client.get_cpu_cores()
        print('===========完成cpu核数采集！===========')
        load_5m = client.get_load_5m()
        print('===========完成5分钟负载采集！===========')
        cpu_usage = client.get_cpu_usage()
        print('===========完成5分钟CPU平均使用率采集！===========')
        mem_usage = client.get_mem_usage()
        print('===========完成内存使用率采集！===========')
        disk_usage = client.get_disk_usage()
        print('===========完成磁盘分区使用率采集！===========')
        disk_read_5m = client.get_disk_read_5m()
        print('===========完成5分钟平均磁盘读取速率采集！===========')
        disk_write_5m = client.get_disk_write_5m()
        print('===========完成5分钟平均磁盘写入速率采集！===========')
        tcp_connections = client.get_tcp_connections()
        print('===========完成TCP连接数采集！===========')
        download_bandwidth_5m = client.get_download_bandwidth_5m()
        print('===========完成5分钟平均下载带宽采集！===========')
        upload_bandwidth_5m = client.get_upload_bandwidth_5m()
        print('===========完成5分钟平均上传带宽！===========')
        file_handler_count = client.get_file_handler_count()
        print('===========完成文件描述符数量采集！===========')

        # 组合数据导出excel
        ip_list = []
        save_data_li = []
        for ip, values in hostname.items():
            if ip not in ip_list:
                ip_list.append(ip)
                save_data_li.append({
                    'ip': ip,
                    'hostname': hostname.get(ip),
                    'uptime': uptime.get(ip),
                    'mem': mem.get(ip),
                    'cpu_cores': cpu_cores.get(ip),
                    'load_5m': load_5m.get(ip),
                    'cpu_usage': cpu_usage.get(ip),
                    'mem_usage': mem_usage.get(ip),
                    'disk_usage': disk_usage.get(ip),
                    'disk_read_5m': disk_read_5m.get(ip),
                    'disk_write_5m': disk_write_5m.get(ip),
                    'tcp_connections': tcp_connections.get(ip),
                    'download_bandwidth_5m': download_bandwidth_5m.get(ip),
                    'upload_bandwidth_5m': upload_bandwidth_5m.get(ip),
                    'file_handler_count': file_handler_count.get(ip)
                })
        PrometheusHostApi.export_excel(save_data_li, filename=filename)
