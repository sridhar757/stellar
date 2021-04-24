import os
import sys
import json
import redis
from confluent_kafka import Producer
from confluent_kafka import Consumer
from stellar_base.utils import StellarMnemonic
from stellar_base.keypair import Keypair

c = Consumer({
    'bootstrap.servers': os.getenv("KAFKA"),
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})
p = Producer({'bootstrap.servers': os.getenv("KAFKA")})

try:
    redisclient = redis.StrictRedis(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"))
    redisclient.ping()
    print('Connected!')
except Exception as ex:
    print('Error:', ex)
    exit('Failed to connect, terminating.')

def XLMaddrgen(coin, userid):
        sm = StellarMnemonic("english")
        m = sm.generate()
        kp = Keypair.deterministic(m, lang='english', index=userid)
        addr = kp.address().decode()
        seed = kp.seed().decode()
        print(addr)
        print(seed)
        return addr,seed
        
def chkaddrfordup(coin, addr):
    return redisclient.exists(coin + ":" + addr)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def publish(topic, det):
    p.poll(0)
    p.produce(topic, json.dumps(det).encode('utf-8'), callback=delivery_report)
    # p.flush()

def updateaddrinredis(addr,pkey, coin, userID, userobj, astobj):
    addrKeyAssigned = coin + ":address:public:" + addr
    prvKeyAssigned = coin + ":address:private:" + addr
    setUserOBJKeyAssigned = coin + ":" + addr
    print('Address Key: {}'.format(addrKeyAssigned))
    print('Private Key: {}'.format(prvKeyAssigned))
    print('User Key for Address: {}'.format(setUserOBJKeyAssigned))
    redisclient.rpush(addrKeyAssigned,"{}")
    redisclient.set(prvKeyAssigned,  pkey)
    details = {"user_id":userID, "userobj": userobj, "assetobj":astobj}
    det = json.dumps(details)
    redisclient.set(setUserOBJKeyAssigned,det)
    
def consume_loop(c, topics):
    c.subscribe(topics)
    msg_count = 0
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: {}'.format(msg.value().decode('utf-8')))
        msgVal = msg.value().decode('utf-8')
        jMsgVal=json.loads(msgVal)
        addr,pkey = XLMaddrgen(jMsgVal["coin"], jMsgVal['userid'])
        ckaddr = chkaddrfordup(jMsgVal["coin"],addr)
        if ckaddr == 1 :
            print("1 ckaddr")
            publish(os.getenv("KAFKA_ADDRESS_TOPIC"),jMsgVal)
        else:
            print("0 ckaddr")
            addrDet = {"user":jMsgVal["userid"], "address":addr, "paymentID": "", "userobj":jMsgVal["userobj"], "astobj":jMsgVal["assetobj"]}
            jaddrDet= json.dumps(addrDet)
            publish(os.getenv("KAFKA_ADDRESS_TOPIC"), jaddrDet)
            updateaddrinredis(addr,pkey, jMsgVal["coin"], jMsgVal["userid"], jMsgVal["userobj"], jMsgVal["assetobj"])
    c.close()

topics = [os.getenv("KAFKA_COMMAND_TOPIC")]
consume_loop(c, topics)
