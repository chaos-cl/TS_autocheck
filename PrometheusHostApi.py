# -*- coding:utf-8 -*-
"""
2022.07.19 cuilei@idss-cn.com; 拉取Prometheus接口指标，并做相应计算，输出巡检指标并写入excel。
"""
from prometheus_api_client import PrometheusConnect
import datetime
import pandas as pd
from openpyxl.utils import get_column_letter
from openpyxl.styles import *
import itertools
import numpy as np
import logging
import re
import base64

logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                    filename='server_check.log',
                    filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志 a是追加模式，默认如果不写的话，就是追加模式
                    format='%(asctime)s - %(levelname)s: %(message)s'
                    )


class PrometheusHostApi:

    def __init__(self, url, headers):
        self.url = url
        self.headers = headers
        self.prom_client = PrometheusConnect(url=self.url, headers=self.headers, disable_ssl=True)

    def get_hostname(self):
        """
        获取主机名清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        hostname_query = 'node_uname_info - 0'

        result = {}
        # 发送PromQL查询【主机名】
        res = self.prom_client.custom_query(query=hostname_query)
        # print(data)

        # 循环取出字典里每个IP的主机名，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            hostname = da['metric']['nodename']
            result[ip] = hostname

        # print(result)
        return result

    def get_uptime(self):
        """
        获取运行时间清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        uptime_query = 'sum(time() - node_boot_time_seconds)by(instance)'

        result = {}
        # 发送PromQL查询【运行时间】
        res = self.prom_client.custom_query(query=uptime_query)
        # print(data)

        # 循环取出字典里每个IP的运行时间，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            uptime = float(da['value'][1])
            d = str(int(uptime // (24 * 60 * 60)))
            result[ip] = (d + "天")

        # print(result)
        return result

    def get_mem(self):
        """
        获取内存大小清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        mem_query = 'node_memory_MemTotal_bytes - 0'

        result = {}
        # 发送PromQL查询【内存大小】
        res = self.prom_client.custom_query(query=mem_query)
        # print(data)

        # 循环取出字典里每个IP的内存大小，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            mem = round(int(da['value'][1]) / (1024 ** 3))
            result[ip] = (str(mem) + "GB")

        # print(result)
        return result

    def get_cpu_cores(self):
        """
        获取CPU核数清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        cpu_cores_query = 'count(node_cpu_seconds_total{mode="system"}) by (instance)'

        result = {}
        # 发送PromQL查询【CPU核数】
        res = self.prom_client.custom_query(query=cpu_cores_query)
        # print(data)

        # 循环取出字典里每个IP的CPU核数，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            cpu_cores = int(da['value'][1])
            result[ip] = (str(cpu_cores) + "vCPU")

        # print(result)
        return result

    def get_load_5m(self):
        """
        获取5分钟负载清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        load_5m_query = 'node_load5'

        result = {}
        # 发送PromQL查询【5分钟负载】
        res = self.prom_client.custom_query(query=load_5m_query)
        # print(data)

        # 循环取出字典里每个IP的5分钟负载，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            load_5m = '%.2f' % float(da['value'][1])
            result[ip] = load_5m

        # print(result)
        return result

    def get_cpu_usage(self):
        """
        获取5分钟CPU平均使用率清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        cpu_usage_query = '(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) by (instance)) * 100'

        result = {}
        # 发送PromQL查询【5分钟CPU平均使用率】
        res = self.prom_client.custom_query(query=cpu_usage_query)
        # print(data)

        # 循环取出字典里每个IP的5分钟CPU平均使用率，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            cpu_usage = '%.2f' % float(da['value'][1])
            result[ip] = (str(cpu_usage) + "%")

        # print(result)
        return result

    def get_mem_usage(self):
        """
        获取内存使用率清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        mem_usage_query = '(1 - (node_memory_MemAvailable_bytes / (node_memory_MemTotal_bytes)))* 100'

        result = {}
        # 发送PromQL查询【内存使用率】
        res = self.prom_client.custom_query(query=mem_usage_query)
        # print(data)

        # 循环取出字典里每个IP的内存使用率，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            mem_usage = '%.2f' % float(da['value'][1])
            result[ip] = (str(mem_usage) + "%")

        # print(result)
        return result

    def get_disk_usage(self):
        """
        获取磁盘使用率清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        disk_usage_query = 'max((node_filesystem_size_bytes{fstype=~"ext.?|xfs"}-node_filesystem_free_bytes{fstype=~"ext.?|xfs"}) *100/(node_filesystem_avail_bytes {fstype=~"ext.?|xfs"}+(node_filesystem_size_bytes{fstype=~"ext.?|xfs"}-node_filesystem_free_bytes{fstype=~"ext.?|xfs"})))by(instance)'

        result = {}
        # 发送PromQL查询【磁盘使用率】
        res = self.prom_client.custom_query(query=disk_usage_query)
        # print(data)

        # 循环取出字典里每个IP的磁盘使用率，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            disk_usage = '%.2f' % float(da['value'][1])
            result[ip] = (str(disk_usage) + "%")

        # print(result)
        return result

    def get_disk_read_5m(self):
        """
        获取磁盘5分钟平均读取速率清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        disk_read_5m_query = 'max(rate(node_disk_read_bytes_total [5m])) by (instance)'

        result = {}
        # 发送PromQL查询【5分钟平均磁盘读取速率】
        res = self.prom_client.custom_query(query=disk_read_5m_query)
        # print(data)

        # 循环取出字典里每个IP的磁盘5分钟平均读取速率，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            disk_read_5m = float(da['value'][1])
            if disk_read_5m < 1024 * 1024:
                disk_read_5m = (str('%.2f' % (disk_read_5m / 1024)) + "KBps")
            else:
                disk_read_5m = (str('%.2f' % (disk_read_5m / (1024 * 1024)) + "MBps"))

            result[ip] = disk_read_5m

        # print(result)
        return result

    def get_disk_write_5m(self):
        """
        获取5分钟平均磁盘写入速率清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        disk_write_5m_query = 'max(rate(node_disk_written_bytes_total [5m])) by (instance)'

        result = {}
        # 发送PromQL查询【5分钟平均磁盘写入速率】
        res = self.prom_client.custom_query(query=disk_write_5m_query)
        # print(data)

        # 循环取出字典里每个IP的5分钟平均磁盘写入速率，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            disk_write_5m = float(da['value'][1])
            if disk_write_5m < 1024 * 1024:
                disk_write_5m = (str('%.2f' % (disk_write_5m / 1024)) + "KBps")
            else:
                disk_write_5m = (str('%.2f' % (disk_write_5m / (1024 * 1024)) + "MBps"))

            result[ip] = disk_write_5m

        # print(result)
        return result

    def get_tcp_connections(self):
        """
        获取TCP连接数清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        tcp_connections_query = 'node_netstat_Tcp_CurrEstab  - 0'

        result = {}
        # 发送PromQL查询【TCP连接数】
        res = self.prom_client.custom_query(query=tcp_connections_query)
        # print(data)

        # 循环取出字典里每个IP的TCP连接数，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            tcp_connections = int(da['value'][1])
            result[ip] = tcp_connections

        # print(result)
        return result

    def get_download_bandwidth_5m(self):
        """
        获取5分钟平均下载带宽清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        download_bandwidth_5m_query = 'max(rate(node_network_receive_bytes_total [5m])*8) by (instance)'

        result = {}
        # 发送PromQL查询【5分钟平均下载带宽】
        res = self.prom_client.custom_query(query=download_bandwidth_5m_query)
        # print(data)

        # 循环取出字典里每个IP的5分钟平均下载带宽，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            download_bandwidth_5m = float(da['value'][1])
            if download_bandwidth_5m < 1024 * 1024:
                download_bandwidth_5m = (str('%.2f' % (download_bandwidth_5m / 1024)) + "KBps")
            else:
                download_bandwidth_5m = (str('%.2f' % (download_bandwidth_5m / (1024 * 1024)) + "MBps"))
            result[ip] = download_bandwidth_5m

        # print(result)
        return result

    def get_upload_bandwidth_5m(self):
        """
        获取5分钟平均上传带宽清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        upload_bandwidth_5m_query = 'max(rate(node_network_transmit_bytes_total [5m])*8) by (instance)'

        result = {}
        # 发送PromQL查询【5分钟平均上传带宽】
        res = self.prom_client.custom_query(query=upload_bandwidth_5m_query)
        # print(data)

        # 循环取出字典里每个IP的5分钟平均上传带宽，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            upload_bandwidth_5m = float(da['value'][1])
            if upload_bandwidth_5m < 1024 * 1024:
                upload_bandwidth_5m = (str('%.2f' % (upload_bandwidth_5m / 1024)) + "KBps")
            else:
                upload_bandwidth_5m = (str('%.2f' % (upload_bandwidth_5m / (1024 * 1024)) + "MBps"))
            result[ip] = upload_bandwidth_5m

        # print(result)
        return result

    def get_file_handler_count(self):
        """
        获取文件描述符数量清单
        :return: {'IP' : value}
        """
        # 定义promQL查询语句
        file_handler_count_query = 'avg(node_filefd_allocated) by (instance)'

        result = {}
        # 发送PromQL查询【文件描述符数量】
        res = self.prom_client.custom_query(query=file_handler_count_query)
        # print(data)

        # 循环取出字典里每个IP的文件描述符数量，最后存入result字典
        for da in res:
            ip = da['metric']['instance'].replace(':9100', '')
            file_handler_count = int(da['value'][1])
            result[ip] = file_handler_count

        # print(result)
        return result


def color_returns(val):
    # 判断是cpu 内存 磁盘占比还是磁盘、网络读写
    # 颜色默认值设置： #FFFFFF
    color = '#FFFFFF'  # black
    if '%' in val:
        mark = re.findall(r'\d+\.?\d*', val)
        if float(mark[0]) >= 80.0:  # 测试用了20.0 ,实际用80。0
            color = '#FF7621'  # light red
        else:
            pass
    elif 'MBps' in val:  #
        mark = re.findall(r'\d+\.?\d*', val)
        if float(mark[0]) >= 80.0:
            color = '#FF7621'  # light red
        else:
            pass
    return f'background-color: {color}'


def export_excel(export, filename):
    """
    将采集到的数据导出excel
    :param export: 数据集合
    :param filename: 文件名
    :return:
    """
    try:
        # 将字典列表转换为DataFrame
        pf = pd.DataFrame(list(export))
        # 指定字段顺序
        order = ['ip',
                 'hostname',
                 'uptime',
                 'mem',
                 'cpu_cores',
                 'load_5m',
                 'cpu_usage',
                 'mem_usage',
                 'disk_usage',
                 'disk_read_5m',
                 'disk_write_5m',
                 'tcp_connections',
                 'download_bandwidth_5m',
                 'upload_bandwidth_5m',
                 'file_handler_count',
                 ]
        pf = pf[order]
        # 将列名替换为中文
        columns_map = {
            'ip': 'IP地址',
            'hostname': '主机名',
            'uptime': '运行时间',
            'mem': '主机内存',
            'cpu_cores': 'CPU核数',
            'load_5m': '最近5分钟平均负载',
            'cpu_usage': 'CPU使用率',
            'mem_usage': '内存使用率',
            'disk_usage': '分区使用率',
            'disk_read_5m': '5分钟平均磁盘读取',
            'disk_write_5m': '5分钟平均磁盘写入',
            'tcp_connections': 'TCP连接数',
            'download_bandwidth_5m': '入站带宽',
            'upload_bandwidth_5m': '出站带宽',
            'file_handler_count': '文件句柄使用'
        }
        pf.rename(columns=columns_map, inplace=True)
        # print(writer_name)
        file_writer = pd.ExcelWriter(filename, engine='openpyxl')
        # 替换空单元格
        pf.fillna(' ', inplace=True)
        # 输出
        pf.to_excel(file_writer, encoding='utf-8', index=False, sheet_name='主机巡检清单')
        # 设置表格格式 , 自动列宽
        column_width = (
            pf.columns.to_series().apply(lambda x: len(x.encode('gbk'))).values
        )
        max_width = (
            pf.astype(str).applymap(lambda x: len(x.encode('gbk'))).agg(max).values
        )
        widths = np.max((column_width, max_width), axis=0)
        worksheet = file_writer.sheets['主机巡检清单']
        for i, width in enumerate(widths, 1):
            worksheet.column_dimensions[get_column_letter(i)].width = width + 2

        # 设置表格格式：添加边框
        border_set = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        for cell in itertools.chain(*worksheet):
            if cell.value is None:
                pass
            else:
                cell.border = border_set
        #
        pf.to_excel(file_writer, encoding='utf-8', index=False, sheet_name='主机巡检清单')
        # 对列进行指标着色：
        # CPU、内存、磁盘使用率>80% 红色
        # 磁盘读写速率、网络传输速度超过80 MBps 红色
        pf0 = pf.style.applymap(color_returns,
                                subset=['CPU使用率', '内存使用率', '分区使用率', '5分钟平均磁盘读取', '5分钟平均磁盘写入', '入站带宽', '出站带宽'])
        # 输出
        pf0.to_excel(file_writer, encoding='utf-8', index=False, sheet_name='主机巡检清单')
        # 保存表格
        file_writer.save()
    except Exception as e:
        print(e)
        logging.exception(e)
