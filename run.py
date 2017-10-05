#-*- coding: utf-8 -*-
from wxpy import *
import os
import sys
import json
import random
import time
import socket
import threading
from multiprocessing import Process
from wx_xnr_es import WX_XNR_ES

WX_XNR_Bot = {}
config = {}

def load_config():
    with open('wx_xnr_conf.json', 'r') as f:
        return json.load(f)

def init_es():
    es_groupmsg = WX_XNR_ES(host=config['es_host'], index_name=config['es_wx_xnr_groupmsg_index_name'], doc_type=config['es_wx_xnr_groupmsg_index_type'])
    es_groupmsg.create_index()
    es_groupmsg.put_mapping(doc_type=es_groupmsg.doc_type, mapping=config['wx_xnr_groupmsg_mapping'])
    return es_groupmsg

def load_groups(bot_id):
    bot = WX_XNR_Bot[bot_id]
    groups = bot.groups(update=True)
    data = []
    for group in groups:
        data.append((group.puid, group.name))
    return data

def load_group_members(bot_id, puid):
    bot = WX_XNR_Bot[bot_id]
    data = []
    for member in ensure_one(WX_XNR_Bot[bot_id].search(puid=puid)).members:
        data.append((member.puid, member.name))
    print len(data)
    print sys.getsizeof(data)   #3312, 而实际上通过print conn.send(result)发现发送的实际大小是24832
    # return 'True'
    return data

def push_msg_by_puid(bot_id, puid, m):
    try:
        ensure_one(WX_XNR_Bot[bot_id].search(puid=puid)).send(m)
        return 'true'
    except Exception,e:
        print e
        return 'false'
    #save sent msg?

def tcplink(conn, addr):
    print 'Accept new connection from %s:%s...'  % addr
    data = conn.recv(config['buffer_size'])
    result = None
    if data:
        data = json.loads(data)
        opt = data['opt']
        if opt == 'loadgroups':
            result = load_groups(data['bot_id'])
        elif opt == 'pushmsgbypuid':
            result = push_msg_by_puid(bot_id=data['bot_id'], puid=data['to_group_puid'], m=data['m'])
        elif opt == 'loadgroupmembers':
            result = load_group_members(bot_id=data['bot_id'], puid=data['group_puid'])
        #发送结果给客户端
        print sys.getsizeof(result)
        print sys.getsizeof(result)/config['buffer_size'] + 1
        # for i in range()
        print conn.send(json.dumps(result))
        # conn.send(json.dumps('True'))
    conn.close()  
    print 'Connection from %s:%s closed.' % addr
    
def control_bot():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((config['socket_host'], config['socket_port']))
    server.listen(5)    #等待连接的最大数量为5
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=tcplink, args=(conn, addr))
        t.start()
 
def save_msg(msg, bot, es_groupmsg):
    d = {'xnr_id': bot.self.puid, 'xnr_name': bot.self.name, 'group_id': msg.sender.puid, 'group_name': msg.sender.name}
    if msg.type == 'Text':
        data = d
        data['msg_type'] = 'Text'
        data['text'] = msg.text
        data['timestamp'] = msg.raw['CreateTime']
        data['speaker_id'] = msg.member.puid
        data['speaker_name'] = msg.member.name
        es_groupmsg.save_data(doc_type=es_groupmsg.doc_type, data=data)  #保存群文本消息到es数据库中
    if msg.is_at:
        time.sleep(random.random())
        msg.reply(u'知道啦~')

def main():
    global config
    config = load_config()
    es_groupmsg = init_es()
    for i in range(config['bot_num']):
        bot_id = 'bot_' + str(i+1)
        print 'starting %s ...' % bot_id
        cache_path = os.path.join('temp', bot_id + '.pkl')
        puid_path = os.path.join('temp', bot_id + '_puid.pkl')
        try:
            bot = Bot(console_qr=True, cache_path=cache_path)
        except Exception, e:
            print e
            os.remove(cache_path)
            bot = Bot(console_qr=True, cache_path=cache_path)
        bot.enable_puid(puid_path)
        WX_XNR_Bot[bot_id] = bot

    #注册messages处理函数
    bot_1 = WX_XNR_Bot['bot_1']
    @bot_1.register(Group)
    def proc_group_msg(msg):
        save_msg(msg, bot_1, es_groupmsg)

    # bot_2 = WX_XNR_Bot['bot_2']
    # @bot_2.register(Group)
    # def proc_group_msg(msg):
    #     save_msg(msg, bot_2, es_groupmsg)

    #开启所有的wxbot之后，主进程监听有没有要主动发送消息的任务
    print 'ready to publish messages ...'
    control_bot()  

if __name__ == '__main__':
    main()