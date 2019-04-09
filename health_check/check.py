#!/usr/bin/env python

import math
import json
import urllib
import urllib2
import logging
import time
import datetime
import base64
import struct
import yaml
import re
from datetime import timedelta
from collections import OrderedDict


logger = logging.getLogger(__name__)


with open('./inventory.ini', 'r') as f:
    _list = f.readlines()
    _idx = _list.index('[monitoring_servers]\n')
    _line = _list[_idx + 1].replace("\n", "")
    _regex = re.compile('\s+')
    _fields = _regex.split(_line)
    if len(_fields) > 1:
        host = _fields[1].split('=')[-1]
    else:
        host = _fields[0]

inventory = [item for item in _list if 'tidb_version' in item or 'cluster_name' in item] 

with open('./group_vars/monitoring_servers.yml', 'r') as f:
    _list = yaml.load(f)
    port = _list['prometheus_port']

BASE_URL = "http://%s:%s/api/v1/query_range?query=" % (host, port)

TITLES = ['sysinfo', 'clsinfo', 'sysload', 'pd', 'tidb', 'ticlient', 'tikv']

TABLES = {
            'sysinfo': ['node_count', 'node_uname', 'node_cpu',
                        'node_mem', 'node_fs'],
            'clsinfo': ['store_capacity', 'region_count',
                        'store_size', 'duration_ms',
                        'qps_by_instance', 'connection_count'],
            'sysload': ['cpu_usage', 'load', 'mem_free',
                        'swap_free', 'fs_free', 'io_util',
                        'iops_read', 'iops_write', 'network_recv',
                        'network_trans', 'tcp_retrans'],
            'pd': ['pd_role', 'cluster_status', 'region_status',
                        'hotspot_status', 'leader_changes'],
            'tidb': ['statement_ops', 'transaction_ops',
                        'slow_query', 'expensive_query',
                        'failed_query', 'tidb_panic',
                        'tidb_critical_err'],
            'ticlient': ['kv_retry',
                        'ticlient_region_err',
                        'kv_backoff', 'lock_resolve'],
            'tikv': ['write_stall', 'scheduler_busy',
                        'tikv_report_failures', 'raftstore_error',
                        'scheduler_error', 'coprocessor_error',
                        'grpc_msg_error', 'leader_drop',
                        'leader_missing', 'channel_full'],
            }

METRICS_MAP = {
            # cluster info
            'store_capacity': 'tikv_store_size_bytes',
            'region_count': 'tikv_pd_heartbeat_tick_total',
            'store_size': 'tikv_engine_size_bytes',
            'duration_ms': 'tidb_server_handle_query_duration_seconds_bucket',
            'qps_by_instance': 'tidb_server_query_total',
            'connection_count': 'tidb_server_connections',
            # sys info
            'node_count': 'probe_success',
            'node_uname': 'node_uname_info',
            'node_cpu': 'node_cpu',
            'node_mem': 'node_memory_MemTotal',
            'node_fs': 'node_filesystem_size',
            # sys load
            'cpu_usage': 'node_cpu',
            'load': 'node_load5',
            'mem_free': 'node_memory_MemAvailable',
            'swap_free': 'node_memory_SwapFree',
            'fs_free': 'node_filesystem_free',
            'io_util': 'node_disk_io_time_ms',
            'iops_read': 'node_disk_reads_completed',
            'iops_write': 'node_disk_writes_completed',
            'network_recv': 'node_network_receive_bytes',
            'network_trans': 'node_network_transmit_bytes',
            'tcp_retrans': 'node_netstat_TcpExt_TCPSynRetrans',
            # tidb
            'statement_ops': 'tidb_executor_statement_total',
            'transaction_ops': 'tidb_session_transaction_total',
            'slow_query': 'tidb_server_slow_query_process_duration_seconds_count',
            'expensive_query': 'tidb_executor_expensive_total',
            'failed_query': 'tidb_server_execute_error_total',
            'tidb_panic': 'tidb_server_panic_total',
            'tidb_critical_err': 'tidb_server_critical_error_total',
            # ticlient
            'kv_retry': 'tidb_tikvclient_backoff_seconds_bucket',
            'ticlient_region_err': 'tidb_tikvclient_region_err_total',
            'kv_backoff': 'tidb_tikvclient_backoff_total',
            'lock_resolve': 'tidb_tikvclient_lock_resolver_actions_total',
            # tikv
            'write_stall': 'tikv_engine_write_stall',
            'scheduler_busy': 'tikv_scheduler_too_busy_total',
            'tikv_report_failures': 'tikv_server_report_failure_msg_total',
            'raftstore_error': 'tikv_storage_engine_async_request_total',
            'scheduler_error': 'tikv_scheduler_stage_total',
            'coprocessor_error': 'tikv_coprocessor_request_error',
            'grpc_msg_error': 'tikv_grpc_msg_fail_total',
            'leader_drop': 'tikv_pd_heartbeat_tick_total',
            'leader_missing': 'tikv_raftstore_leader_missing',
            'channel_full': 'tikv_channel_full_total',
            # pd
            'pd_role': 'pd_server_tso',
            'cluster_status': 'pd_cluster_status',
            'region_status': 'pd_regions_status',
            'hotspot_status': 'pd_hotspot_status',
            'leader_changes': 'etcd_server_leader_changes_seen_total',
            }

METRICS = OrderedDict()

for t in TITLES:
    for v in TABLES[t]:
        METRICS[v] = METRICS_MAP[v]

_instances = {}
_clusters = {}
_stores = {}
_sysload = {}
_pd = {}
_tidb = {}
_ticlient = {}
_tikv = {}

def query_expression(title):
    m = METRICS[title]
    return {
            'store_size':           "sum(%s)by(job)" % m,
            'store_capacity':       m,
            'region_count':         m,
            'duration_ms':          "histogram_quantile(0.999,sum(rate(%s[1m]))by(le,instance))" % m,
            'qps_by_instance':      "rate(%s[1m])" % m,
            'connection_count':     m,
            'node_count':           "%s{group!=''}" % m,
            'node_uname':           m,
            'node_cpu':             "count(%s{mode='user'})by(instance)" % m,
            'node_mem':             m,
            'node_fs':              m,
            'cpu_usage':            "100-avg%sby(instance)(irate(%s{mode='idle'}[1m]))*100" % ("%20", m),
            'load':                 "node_load5",
            'mem_free':             m,
            'swap_free':            m,
            'fs_free':              m,
            'io_util':              "rate(%s[5m])/1000" % m,
            'iops_read':            "sum%sby(instance)(irate(%s[1m]))" % ("%20", m),
            'iops_write':           "sum%sby(instance)(irate(%s[1m]))" % ("%20", m),
            'network_recv':         "irate(%s{device!='lo'}[5m])*8" % m,
            'network_trans':        "irate(%s{device!='lo'}[5m])*8" % m,
            'tcp_retrans':          "irate(%s[1m])" % m,
            'statement_ops':        "sum(rate(%s[1m]))by(type)" % m,
            'transaction_ops':      "sum(rate(%s[1m]))by(type)" % m,
            'slow_query':           "sum(%s)by(instance)" % m,
            'expensive_query':      "sum(irate(%s[5m]))by(instance)" % m,
            'failed_query':         "sum(increase(%s[1m]))by(type)" % m,
            'tidb_panic':           "increase(%s[1m])" % m,
            'tidb_critical_err':    "increase(%s[1m])" % m,
            'kv_retry':             "histogram_quantile(0.999,sum(rate(%s[1m]))by(le))" % m,
            'ticlient_region_err':  "sum(rate(%s[1m]))by(type)" % m,
            'kv_backoff':           "sum(rate(%s[1m]))by(type)" % m,
            'lock_resolve':         "sum(rate(%s[1m]))by(type)" % m,
            'write_stall':          "avg(%s{type='write_stall_percentile99'})by(instance)" % m,
            'scheduler_busy':       "sum(rate(%s[1m]))by(instance)" % m,
            'channel_full':         "sum(rate(%s[1m]))by(instance,type)" % m,
            'tikv_report_failures': "sum(rate(%s[1m]))by(type,instance,store_id)" % m,
            'raftstore_error':      "sum(rate(%s{status!~'success|all'}[1m]))by(instance,status)" % m,
            'scheduler_error':      "sum(rate(%s{stage=~'snapshot_err|prepare_write_err'}[1m]))by(instance,stage)" % m,
            'coprocessor_error':    "sum(rate(%s[1m]))by(instance,reason)" % m,
            'grpc_msg_error':       "sum(rate(%s[1m]))by(instance,type)" % m,
            'leader_drop':          "sum(delta(%s{type='leader'}[1m]))by(instance)" % m,
            'leader_missing':       "sum(%s)by(instance)" % m,
            'pd_role':              "delta(%s{type='save'}[1m])" % m,
            'cluster_status':       m,
            'region_status':        m,
            'hotspot_status':       m,
            'leader_changes':       m,
            }[title]

def collect_metric(title, start, end, step):
    query_expr = query_expression(title)
    time_range = "&start=%s&end=%s&step=%s" % (start, end, step)
    query_url = BASE_URL + query_expr + time_range
    req = urllib2.Request(query_url)
    res = urllib2.urlopen(req)
    data = json.loads(res.read())
    return data

def analyze_data(title, data):
    assert('result' in data['data'])
    res = data['data']['result']
    for i in res:
        if title == 'qps_by_instance':
            if i['metric']['type'] == 'Query' and i['metric']['result'] == 'OK':
                inet = i['metric']['instance'].split(':')[0]
                node = base64.encodestring(inet).replace("\n", "")
                calc_stats(_clusters, title, node, i['values'])
        elif title in ('duration_ms', 'connection_count'):
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            calc_stats(_clusters, title, node, i['values'])
        elif title == 'node_count':
            group = i['metric']['group']
            if group not in ('tidb', 'tikv', 'pd'):
                continue
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            if node not in _instances:
                _instances[node] = dict() 
            if group not in _instances[node]:
                _instances[node][group] = int(i['values'][0][-1])
            else:
                _instances[node][group] += int(i['values'][0][-1])
        elif title in ('node_uname', 'node_cpu', 'node_mem', 'node_fs'):
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            if node not in _instances:
                continue
            if title == 'node_uname':
                name = i['metric']['nodename']
                _instances[node].setdefault('name', name) 
                _instances[node].setdefault(title, i['metric']['release']) 
            elif title in ('node_cpu', 'node_mem'):
                _instances[node].setdefault(title, i['values'][0][-1])
            elif title == 'node_fs':
                _instances[node].setdefault(title, [])
                for d in ('device', 'fstype','mountpoint'):
                    _instances[node][title].append(i['metric'][d])
                _instances[node][title].append(i['values'][0][-1])
        elif title in ('region_count', 'store_capacity'):
            kv = i['metric']['job']
            _type = i['metric']['type']
            name = i['metric']['instance']
            if name not in _stores:
                _stores[name] = dict()
            _stores[name].setdefault(title, {})
            _stores[name][title].setdefault(kv, [])
            _stores[name][title][kv].append({_type: i['values'][-1][-1]})
        elif title == 'store_size':
             for k0, v in _stores.items():
                 for k2 in v['store_capacity']:
                     if k2 == i['metric']['job']:
                         _stores[k0]['store_capacity'][k2].append({title: i['values'][-1][-1]})
        elif title in ('cpu_usage', 'load', 'mem_free', 'swap_free', 'fs_free', 'io_util', 'iops_read', 'iops_write', 'tcp_retrans'):
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            if node not in _instances:
                continue
            name = _instances[node]['name']
            if title == 'fs_free':
                if i['metric']['device'] <> _instances[node]['node_fs'][0]:
                    continue
                suf = _instances[node]['node_fs'][2][1:]
                suf = suf if suf else 'root'
                _title = title + '_%s' % suf
                calc_stats(_sysload, _title, name, i['values'])
            elif title == 'io_util':
                suf = i['metric']['device']
                _title = title + '_%s' % suf
                calc_stats(_sysload, _title, name, i['values'])
            else:
                calc_stats(_sysload, title, name, i['values'])
        elif title in ('network_recv', 'network_trans'):
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            if node not in _instances:
                continue
            name = _instances[node]['name']
            suf = i['metric']['device']
            _title = title + '_%s' % suf
            calc_stats(_sysload, _title, name, i['values'])
        elif title == 'pd_role':
            if float(i['values'][-1][-1]) > 0:
                _pd['pd_leader'] = i['metric']['job']
        elif title in ('cluster_status','region_status'):
            if i['metric']['job'] == _pd['pd_leader']:
                calc_stats(_pd, i['metric']['type'], title, i['values'])
        elif title == 'hotspot_status':
            name = i['metric']['type'] + '_' + i['metric']['store']
            calc_stats(_pd, name, title, i['values'])
        elif title == 'leader_changes':
            calc_stats(_pd, i['metric']['job'], title, i['values'])
        elif title in ('statement_ops', 'transaction_ops', 'failed_query'):
            calc_stats(_tidb, i['metric']['type'], title, i['values'])
        elif title in ('slow_query', 'expensive_query', 'tidb_panic', 'tidb_critical_err'):
            inet = i['metric']['instance'].split(':')[0]
            node = base64.encodestring(inet).replace("\n", "")
            calc_stats(_tidb, node, title, i['values'])
        elif title == 'kv_retry':
            vv = []
            for l in i['values']:
                if 'NaN' not in l:
                    vv.append(l)
            if not vv:
                vv= [[0, 0]]
            calc_stats(_ticlient, title, title, vv)
        elif title in ('ticlient_region_err', 'kv_backoff', 'lock_resolve'):
            calc_stats(_ticlient, i['metric']['type'], title, i['values'])
        elif title in ('write_stall', 'scheduler_busy'):
            calc_stats(_tikv, i['metric']['instance'], title, i['values'])
        elif title == 'tikv_report_failures':
            name = i['metric']['instance'] + '_' + i['metric']['store_id'] + '_' + i['metric']['type']
            calc_stats(_tikv, name, title, i['values'])
        elif title == 'raftstore_error':
            name = i['metric']['instance'] + '_' + i['metric']['status']
            calc_stats(_tikv, name, title, i['values'])
        elif title == 'scheduler_error':
            name = i['metric']['instance'] + '_' + i['metric']['stage']
            calc_stats(_tikv, name, title, i['values'])
        elif title == 'coprocessor_error':
            name = i['metric']['instance'] + '_' + i['metric']['reason']
            calc_stats(_tikv, name, title, i['values'])
        elif title == 'grpc_msg_error':
            name = i['metric']['instance'] + '_' + i['metric']['type']
            calc_stats(_tikv, name, title, i['values'])
        elif title in ('leader_drop', 'leader_missing'):
            name = i['metric']['instance']
            calc_stats(_tikv, name, title, i['values'])
        elif title == 'channel_full':
            name = i['metric']['instance'] + '_' + i['metric']['type']
            calc_stats(_tikv, name, title, i['values'])
        else:
            pass

def to_unixtime(dt):
    return int(time.mktime(dt.timetuple()))

def __avg(vals):
    if len(vals) > 0:
        return sum([float(v[-1]) for v in vals])/len(vals)
    return None 

def __max(vals):
    return max([float(v[-1]) for v in vals])

def __min(vals):
    return min([float(v[-1]) for v in vals])

def calc_stats(cube, title, name, values):
    if name not in cube:
        cube[name] = dict()
    cube[name].setdefault(title, [])
    cube[name][title].append({'current': values[-1][-1]})
    for f in ('max', 'min', 'avg', 'top99'):
        cube[name][title].append({f: eval('__%s' % f)(values)})

def __top99(vals):
    return __topN(vals, 0.99)

def __top90(vals):
    return __topN(vals, 0.9)

def __topN(values, rate):
    vals = sorted([v[1] for v in values])
    if len(vals) > 0:
        return vals[int(len(vals)*rate)]
    return None


if __name__ == '__main__':
    kwargs = OrderedDict()
    end  = to_unixtime(datetime.datetime.now())
    start = to_unixtime(datetime.datetime.now()-datetime.timedelta(hours=12))
    for item in inventory:
         _list = item.split('=')
         kwargs[_list[0].strip()] = _list[-1].replace('\n', '').strip()
    for item in METRICS:
        result = collect_metric(item, start, end, 15)
        analyze_data(item, result)
    kwargs['snapshot'] = '%s - %s' % (start, end)
    kwargs['sysinfo'] = _instances
    kwargs['clsinfo'] = _clusters
    kwargs['stoinfo'] = _stores
    kwargs['sysload'] = _sysload
    kwargs['pd'] = _pd
    kwargs['tidb'] = _tidb
    kwargs['ticlient'] = _ticlient
    kwargs['tikv'] = _tikv
    output = r'/tmp/%s_%s_health_check.json' % (kwargs['cluster_name'], kwargs['tidb_version'])
    with open(output, 'w+') as f:
        json.dump(kwargs, f)
