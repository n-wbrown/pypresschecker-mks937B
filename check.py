# nwbrown
# 191222
# Execute this script to check the pressures on GMD during winter closure.
# For intended performance, execute this script on psdev after runing this:
#   source /reg/g/pcds/pyps/conda/py36env.sh

import re
import time 
from  telnetlib import Telnet
from typing import Union
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(funcName)20s:%(lineno)3d:%(levelname)10s:%(message)s')

logger = logging.getLogger(__name__)

_RESPONSE_READER = re.compile(
    r"(?P<header>@)(?P<addr>\d{1,3})(?P<ack>ACK|NAK)(?P<press>[A-Z0-9.+-_]*)"
    r"(?P<end>;FF)"
)

_gmd_prefix = "EM1K0-GMD"
_xgmd_prefix = "EM2K0-XGMD"
GAUGES = {
    'moxa-kfe-01':{
        '4001':{
           1:f'{_gmd_prefix}-GCC-10',
           3:f'{_gmd_prefix}-GCC-20',
           5:f'{_gmd_prefix}-GCC-30',
        },
        '4002':{
           1:f'{_gmd_prefix}-GCC-50',
           3:f'{_gmd_prefix}-GCC-60',
        },
        '4003':{
           1:f'{_gmd_prefix}-GCC-70',
        },
        '4007':{
           5:f'{_xgmd_prefix}-GPI-50',
        },
    }
}

def telnet_read_press(hostname: str, port: int, channels: list=None,
                      addr: int=253) -> list:
    logging.debug(hostname)
    logging.debug(port)
    logging.debug(channels)
    logging.debug(addr)
    if not channels:
        channels = range(1,7)
    response = {}
    with Telnet(hostname, port) as tn:
        for channel in channels:
            query = "@{addr}PR{ch}?;FF".format(
                addr=addr,
                ch=channel
            ).encode('ascii')
            logging.debug(query)
            tn.write(query)
            try:
                result = str(tn.read_until(";FF".encode("ascii"),timeout=1))
                logger.debug(result)
                result_parsed = _RESPONSE_READER.search(result).groupdict()
                response[channel] = result_parsed
            except EOFError: 
                response[channel] = None

    return response

def extract_press(data: list, channel: int) -> Union[str,float]:
    row = data[channel]
    if row['ack'] == 'NAK':
        return 'Gauge not acknowledged'
    elif row['press'] == 'OFF':
        return "OFF"
    else:
        return float(row['press'])

def bulk_get(structure: dict):
    for ser_server_name in structure:
        for port_name in structure[ser_server_name]:
            port = structure[ser_server_name][port_name]
            data = telnet_read_press(ser_server_name, port_name)
            for channel_name in port:
                channel = port[channel_name]
                name = channel
                value = extract_press(data,channel_name)
                yield (name, value)

def main():
    
    m = bulk_get(GAUGES)

    data = []
    data.append(str(time.ctime()))
    data.append(",")
    for name, value in m:
        #print(name, ": ", value)
        data.append(str(value))
        data.append(",")
    data.append('\n')
    with open('gmd_press_log','a') as f:
        f.write("".join(data))
     



if __name__ == '__main__':
    while 1:
        time.sleep(60)
        main()
    
    print('done')





