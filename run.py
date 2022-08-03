# -*- coding:utf-8 -*-
"""
2022.07.19 cuilei@idss-cn.com; 拉取Prometheus接口指标，并做相应计算，输出巡检指标并写入excel。
"""
import check
import datetime


if __name__ == "__main__":
    # 每个方法对应各自组件的巡检功能， 参数auth='N' 表示prometheus未配置认证，auth='Y',表示配置认证
    # 如果prometheus配置有HTTP Basic 认证，则需要修改config.py中的token参数。并删掉auth参数。
    # 如 check.es_check(filename) ，这样调用即可
    # 注意： host_check()必须放第一个，他会生成excel文件，放到后面可能会导致文件生成异常
    today = datetime.date.today()
    filename = datetime.datetime.strftime(today, '%Y-%m-%d') + '-态势巡检报告.xlsx'
    # 生成指标
    check.host_check(filename, auth='Y')
    check.es_check(filename, auth='Y')
    check.kafka_check(filename, auth='Y')
    check.zookeeper_check(filename, auth='Y')
    check.mysql_check(filename, auth='Y')
    check.redis_check(filename, auth='Y')
