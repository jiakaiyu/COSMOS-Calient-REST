""" Calient S320 Control

Author:   Jiakai Yu (jiakaiyu@email.arizona.edu)
Created:  2019/08/09
Version:  1.0

Last modified by Jiakai: 2020/9/16
"""


from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER,CONFIG_DISPATCHER,DEAD_DISPATCHER,HANDSHAKE_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
import time
from Common import *
import Database
import Custom_event
import logging
from Common import log_level
import json
import re
import psycopg2
import socket
import requests
import json

logging.basicConfig(level = log_level)

HOST="***.***.***.***"
PORT="3082"
USER="***"
PASS="***"


class Calient(object):
    def __init__(self, IP_addr=HOST, USERNAME=USER, PASSWORD=PASS):
        self.ip = IP_addr
        self.username = USERNAME
        self.password = PASSWORD

    def set_crs(self, crs):
        try:
            s = requests
            rpc = s.post('http://{}/rest/crossconnects/?id=badd'.format(self.ip), data=json.dumps(crs), auth=(self.username, self.password))
            data = ''.join(str(rpc.content.decode('utf-8')))
            jdata = json.loads(data)
            for res in jdata:
                if res['response']['status'] == "0":
                    return False
            return True
        except Exception as e:
            return False

    def tear_crs(self, crs):
        try:
            s = requests
            rpc = s.post('http://{}/rest/crossconnects/?id=bdel'.format(self.ip), data=json.dumps(crs), auth=(self.username, self.password))
            data = ''.join(str(rpc.content.decode('utf-8')))
            jdata = json.loads(data)
            for res in jdata:
                if res['response']['status'] == "0":
                    return False
            return True
        except Exception as e:
            return False

    def get_crs(self):
        links = []
        try:
            s = requests
            rpc = s.get('http://{}/rest/crossconnects/?id=list'.format(self.ip), auth=(self.username, self.password))
            data = ''.join(str(rpc.content.decode('utf-8')))
            link_data = json.loads(data)
            for line in link_data:
                link = line['half1']['conn']
                links.append(link)
            return links
        except Exception as e:
            return links


class calient_rest(app_manager.RyuApp):

    _EVENTS =  [Custom_event.SetupCRSEvent,
                Custom_event.SetupCRSReplyEvent,
                Custom_event.TeardownCRSEvent,
                Custom_event.TeardownCRSReplyEvent,
                Custom_event.GetTopoCRSEvent,
                Custom_event.GetTopoCRSReplyEvent,
                Custom_event.SaveTopoCRSEvent,
                Custom_event.SaveTopoCRSReplyEvent,
                Custom_event.LoadTopoCRSEvent,
                Custom_event.LoadTopoCRSReplyEvent]

    def __init__(self,*args,**kwargs):
        super(calient_rest,self).__init__(*args,**kwargs)

    @set_ev_cls(Custom_event.SetupCRSEvent)
    def _SetupCRS(self,ev):
        ev_reply = Custom_event.SetupCRSReplyEvent()
        ev_reply.crs = ev.crs
        calient = Calient()
        crs = []
        for c in ev.crs:
            _in = re.split('(-|>)', c)[0]
            _dir = re.split('(-|>)', c)[1]
            _out = re.split('(-|>)', c)[2]
            if _dir == "-":
                _dir = 'bi'
            else:
                _dir = "uni"
            crs.append({"in": _in, "out": _out, "dir": _dir})
        result = calient.set_crs(crs)
        if result:
            ev_reply.result = SUCCESS
        else:
            ev_reply.result = FAIL
        self.send_event('Intra_domain_***',ev_reply)

    @set_ev_cls(Custom_event.TeardownCRSEvent)
    def _TeardownCRS(self,ev):
        ev_reply = Custom_event.TeardownCRSReplyEvent()
        ev_reply.crs = ev.crs
        calient = Calient()
        crs = []
        for c in ev.crs:
            crs.append({"conn": c})
        result = calient.tear_crs(crs)
        if result:
            ev_reply.result = SUCCESS
        else:
            ev_reply.result = FAIL
        self.send_event('Intra_domain_***',ev_reply)

    @set_ev_cls(Custom_event.GetTopoCRSEvent)
    def _GetTopo(self,ev):
        ev_reply = Custom_event.GetTopoCRSReplyEvent()
        calient = Calient()
        links = calient.get_crs()
        ev_reply.topo = links
        if links:
            ev_reply.result = SUCCESS
        else:
            ev_reply.result = FAIL
        self.send_event('Intra_domain_***',ev_reply)

        if links:
            conn = psycopg2.connect(database='***', user='***', password='***', host='***')
            cursor = conn.cursor()
            query_truncate = '''TRUNCATE TABLE s320_***;'''
            cursor.execute(query_truncate)
            for link in links:
                p_a = re.split('(-|>)', link)[0]
                _dir = re.split('(-|>)', link)[1]
                p_b = re.split('(-|>)', link)[2]
                query = '''
                        INSERT into
                            s320_***
                            (Port_A, Port_B, direction)
                        VALUES
                            ('{}','{}','{}')
                        '''.format(p_a, p_b, _dir)
                cursor.execute(query)
                query_clean = '''
                    DELETE FROM s320_*** a using s320_*** b
                    WHERE a.ctid < b.ctid
                    AND a.Port_A = b.Port_A
                    AND A.Port_B = b.Port_b
                    '''
                cursor.execute(query_clean)
            conn.commit()
            conn.close()
