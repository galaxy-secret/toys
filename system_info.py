# coding: utf-8
# __author__ = duanbin

'''
将 系统信息保存到 mongodb中
'''

import time
import subprocess

from pymongo import MongoClient

mongo_config = {
    "host": "127.0.0.1",
    "port": 27017,
    "username": "root",
    "password": "root",
}
mongo = MongoClient(host=mongo_config['host'], port=mongo_config['port'],
                    username=mongo_config['username'], password=mongo_config['password'])

last_net_info = None
last_cpu_info = None
last_io_info = None
last_tcp_info = None

process_names = ['solr']

def collect_loadavg():
    '''
    采集负载信息
    '''
    f_loadavg = open("/proc/loadavg")
    loadavg_info = f_loadavg.read().split()
    f_loadavg.close()
    loadavg = {
        'process_nums_1_per_minute': float(loadavg_info[0]),
        'process_nums_5_per_minute': float(loadavg_info[1]),
        'process_nums_15_per_minute': float(loadavg_info[2]),
        'process_running_per_total': loadavg_info[3],
        'lastest_pid': int(loadavg_info[4]),
    }
    return loadavg

def collect_meminfo():
    '''
    采集 内存信息
    '''
    meminfo = {}
    with open("/proc/meminfo") as f_meminfo:
        for line in f_meminfo.readlines():
            contents = line.split(":")
            meminfo[contents[0]] = contents[1].strip()
    return meminfo


def collect_cpu_info():
    '''收集cpu信息'''
    cpu_buffer = {}
    with open("/proc/stat") as cpu_file:
        for line in cpu_file:
            line_fields = line.split()
            if line_fields[0].startswith("cpu"):
                total = 0
                for field in line_fields:
                    if field.startswith("cpu"):
                        continue
                    total += int(field)
                cpu_buffer[line_fields[0]] = {
                    "User": int(line_fields[1]),
                    "Sys": int(line_fields[3]),
                    "Idle": int(line_fields[4]),
                    "Steal": int(line_fields[8]),
                    "Wait": int(line_fields[5]),
                    "Total": total
                }

    return cpu_buffer


def calculate_cpu_info():
    '''计算cpu信息'''
    global last_cpu_info
    cpu_info = collect_cpu_info()
    res = {}
    if last_cpu_info is not None:
        for k, value in cpu_info.items():
            delta_total = value["Total"] - last_cpu_info[k]["Total"]
            delta_user = value["User"] - last_cpu_info[k]["User"]
            delta_sys = value["Sys"] - last_cpu_info[k]["Sys"]
            delta_idle = value["Idle"] - last_cpu_info[k]["Idle"]
            delta_wait = value["Wait"] - last_cpu_info[k]["Wait"]
            delta_steal = value["Steal"] - last_cpu_info[k]["Steal"]
            res[k] = {
                "cpu_user": int(float(delta_user) / float(delta_total) * 10000),
                "cpu_sys": int(float(delta_sys) / float(delta_total) * 10000),
                "cpu_wait": int(float(delta_wait) / float(delta_total) * 10000),
                "cpu_steal": int(float(delta_steal) / float(delta_total) * 10000),
                "cpu_idle": int(float(delta_idle) / float(delta_total) * 10000),
                "cpu_util": int(float(delta_total - delta_idle - delta_wait - delta_steal) / float(delta_total) * 10000)
            }
    last_cpu_info = cpu_info
    return res


def collect_io_info():
    '''收集io信息'''
    io_buffer = {}
    with open("/proc/diskstats") as io_file:
        for line in io_file:
            line_fields = line.split()
            device_name = line_fields[2]
            if line_fields[3] == "0":
                continue
            if should_handle_device(device_name):
                io_buffer[device_name] = {
                    "ReadRequest": int(line_fields[3]),
                    "WriteRequest": int(line_fields[7]),
                    "MsecRead": int(line_fields[6]),
                    "MsecWrite": int(line_fields[10]),
                    "MsecTotal": int(line_fields[12]),
                    "Timestamp": int(time.time())
                }
    return io_buffer


def should_handle_device(device):
    '''当前的硬盘设备是否需要使用'''
    normal = len(device) == 3 and device.startswith(
        "sd") or device.startswith("vd")
    aws = len(device) >= 4 and device.startswith(
        "xvd") or device.startswith("sda")
    return normal or aws


def calculate_io_info():
    '''计算io数据'''
    global last_io_info
    io_info = collect_io_info()
    result = {}
    if last_io_info is not None:
        for key in io_info.keys():
            total_duration = io_info[key]["Timestamp"] - \
                last_io_info[key]["Timestamp"]
            read_use_io = io_info[key]["MsecRead"] - \
                last_io_info[key]["MsecRead"]
            write_use_io = io_info[key]["MsecWrite"] - \
                last_io_info[key]["MsecWrite"]
            read_io = io_info[key]["ReadRequest"] - \
                last_io_info[key]["ReadRequest"]
            write_io = io_info[key]["WriteRequest"] - \
                last_io_info[key]["WriteRequest"]
            read_write_io = io_info[key]["MsecTotal"] - \
                last_io_info[key]["MsecTotal"]
            readwrite_io = read_io + write_io
            io_awit = 0
            if readwrite_io > 0:
                io_awit = int(float(read_use_io + write_use_io) /
                              float(readwrite_io) * 10000)
            result[key] = {
                "io_rs": int((read_io / total_duration) * 10000),
                "io_ws": int((write_io / total_duration) * 10000),
                "io_await": io_awit,
                "io_util": int(float(read_write_io) / (total_duration * 1000) * 10000),
            }

    last_io_info = io_info
    return result


def collect_net_info():
    '''收集网卡信息'''
    net_buffer = {}
    with open("/proc/net/dev") as net_file:
        for line in net_file:
            if line.find(":") < 0:
                continue
            card_name = line.split(":")[0].strip()
            if should_collect_card(card_name):
                line_fields = line.split(":")[1].lstrip().split()
                net_buffer[card_name] = {
                    "InBytes": int(line_fields[0]),
                    "InPackets": int(line_fields[1]),
                    "InErrors": int(line_fields[2]),
                    "InDrops": int(line_fields[3]),
                    "OutBytes": int(line_fields[8]),
                    "OutPackets": int(line_fields[9]),
                    "OutErrors": int(line_fields[10]),
                    "OutDrops": int(line_fields[11])
                }
    return net_buffer


def should_collect_card(line):
    '''是否需要采集相应的网卡'''
    return line.startswith("eth") or line.startswith("em") or line.startswith("eno")


def calculate_net_info():
    '''计算网卡指标'''
    global last_net_info
    net_info = collect_net_info()
    result = {}
    if last_net_info is not None:
        for key in net_info.keys():
            result[key] = {
                "in_bytes": (net_info[key]["InBytes"] - last_net_info[key]["InBytes"]) * 10000,
                "in_packets": (net_info[key]["InPackets"] - last_net_info[key]["InPackets"]) * 10000,
                "in_errors": (net_info[key]["InErrors"] - last_net_info[key]["InErrors"]) * 10000,
                "in_drops": (net_info[key]["InDrops"] - last_net_info[key]["InDrops"]) * 10000,
                "out_bytes": (net_info[key]["OutBytes"] - last_net_info[key]["OutBytes"]) * 10000,
                "out_packets": (net_info[key]["OutPackets"] - last_net_info[key]["OutPackets"]) * 10000,
                "out_errors": (net_info[key]["OutErrors"] - last_net_info[key]["OutErrors"]) * 10000,
                "out_drops": (net_info[key]["OutDrops"] - last_net_info[key]["OutDrops"]) * 10000
            }
    last_net_info = net_info
    return result


def collect_tcp_info():
    '''采集tcp相关数据'''
    tcp_buffer = {}
    is_title = True
    with open("/proc/net/snmp") as tcp_file:
        for line in tcp_file:
            protocol_name = line.split(":")[0].strip()
            if protocol_name == "Tcp":
                if is_title:
                    is_title = False
                    continue
                else:
                    line_fields = line.split(":")[1].lstrip().split()
                    tcp_buffer = {
                        "ActiveOpens": int(line_fields[4]),
                        "PassiveOpens": int(line_fields[5]),
                        "InSegs": int(line_fields[9]),
                        "OutSegs": int(line_fields[10]),
                        "RetransSegs": int(line_fields[11]),
                        "CurrEstab": int(line_fields[8]),
                    }
                    break
    return tcp_buffer


def calculate_tcp_info():
    '''计算tcp数据'''
    global last_tcp_info
    tcp_info = collect_tcp_info()
    result = {}
    if last_tcp_info is not None:
        outSegsTcp = tcp_info["OutSegs"] - last_tcp_info["OutSegs"]
        retransRate = float(
            tcp_info["RetransSegs"] - last_tcp_info["RetransSegs"]) / float(outSegsTcp)
        result = {
            "tcp_active": (tcp_info["ActiveOpens"] - last_tcp_info["ActiveOpens"]) * 10000,
            "tcp_passive": (tcp_info["PassiveOpens"] - last_tcp_info["PassiveOpens"]) * 10000,
            "tcp_inseg": (tcp_info["InSegs"] - last_tcp_info["InSegs"]) * 10000,
            "tcp_outseg": outSegsTcp * 10000,
            "tcp_established": tcp_info["CurrEstab"] * 10000,
            "tcp_retran": int(retransRate * 10000)
        }
    last_tcp_info = tcp_info
    return result



def collect_process_info():
    '''抓取某些进程的信息'''
    global process_names
    process_info = []
    if process_names is not None:
        process_filter = "\|".join(process_names)
        process_filter = "'" + process_filter + "'"
        commandline = "ps aux | grep " + process_filter
        status_code, result = subprocess.getstatusoutput(commandline)
        if status_code == 0:
            # 分割结果
            result_array = result.split("\n")
            for item in result_array:
                item_fields = item.split()
                # 过滤掉 查看 日志 tail 和 grep 命令的干扰 /bin/sh -c ps aux | grep 'solr'
                if item_fields[10].startswith("tail") or item_fields[10].startswith("grep") or " ".join(item_fields[10:]).startswith("/bin/sh -c " + commandline):
                    continue

                process_info.append({'process_name': " ".join(item_fields[10:]),
                                     'process_user': item_fields[0],
                                     "process_cpu_util": float(item_fields[2]),
                                     "process_mem_util": float(item_fields[3])
                                    })
    return process_info



def main():
    '''main'''
    system_info = {}
    system_info['load_avg'] = collect_loadavg()
    system_info['meminfo'] = collect_meminfo()
    system_info['cpu_info'] = calculate_cpu_info()
    system_info['io_info'] = calculate_io_info()
    system_info['net_info'] = calculate_net_info()
    system_info['tcp_info'] = calculate_tcp_info()
    system_info['watched_process_info'] = collect_process_info()
    system_info['timestamp'] = time.time()
    system_info['datetime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    mongo['Monitor']['system_info'].insert_one(system_info)



if __name__ == '__main__':
    num = 0
    max_num = 60 * 60 * 24 * 2 - 1
    # max_num = 1
    while True:
        if num > max_num:
            break
            exit()
        main()
        num = num + 60*15
        # time.sleep(3)
        time.sleep(60*15)
