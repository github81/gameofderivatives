#!/usr/bin/python

#----------------------------------------------------------------
# Purpose:  this script is used to produce fx rates and swap trade messages
#           python produce_stream.py <KAFKA_PRODUCER_HOST> xccy_swaps xccy_swaps
#----------------------------------------------------------------

import random
import sys
import time
import six
import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import json
import random

class Producer(object):
    
    def __init__(self, addr, topic):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)
        self.topic = topic
    
    #function to produce random fx data
    def produce_fx_rates(self, source_symbol):
        ccy = ["EUR","JPY","GBP","AUD","CHF","XAG","XAU","NZD"]
        base_ccy = "USD"
        msg_cnt = 0
        while True:
            rate = random.uniform(0.5,1)
            price_ccy = random.choice(ccy)
            date = datetime.datetime.now().strftime("%-m/%-d/%Y")
            message_info = {'source_stream': "fx_stream",
                'date': str(date),'base': str(base_ccy),
                        'price': str(price_ccy),
                        'rate': str(rate)}
            message_info = json.dumps(message_info)
            print(message_info)
            source_symbol2=''
            self.producer.send_messages(self.topic + "_" + price_ccy, source_symbol2.encode(), message_info.encode())
            msg_cnt += 1
            time.sleep(10)

    def getCounterparty(self,ORGANIZATION):
        counterparty = "org" + str(random.randint(1,10000))
        if counterparty == ORGANIZATION:
            self.getCounterparty(ORGANIZATION)
        else:
            return counterparty

    def format_dash_date(olddate):
        olddate = datetime.datetime.strptime(olddate,'%m/%d/%Y')
        dashdate = datetime.date(olddate.year,olddate.month,olddate.day).strftime('%Y-%m-%d')
        return dashdate
    
    #function to produce random swap trade data
    def produce_xccy_swaps(self, source_symbol):
        index = ["FIXED", "LIBOR"]
        ccy = ["EUR","JPY","GBP","AUD","CHF","XAG","XAU","NZD"]
        longshort = ["LONG","SHORT"]
        term = ["3M","6M","1Y"]
        pay_or_receive = ["pay","receive"]
        msg_cnt = 1
        while True:
            id =  "SWAP" + str(msg_cnt)
            organization = "org" + str(random.randint(1,750))
            fromDate = datetime.datetime.strptime("12/30/2015", '%m/%d/%Y')
            toDate = datetime.datetime.strptime("12/30/2017", '%m/%d/%Y')
            startDays = (toDate-fromDate).days
            startDays = random.randint(0,startDays)
            tradeDate = fromDate + datetime.timedelta(days=startDays)
            tradeDate  = str(tradeDate.strftime('%Y-%m-%d'))
            settlementDate = fromDate + datetime.timedelta(days=(startDays+2))
            settlementDate = str(settlementDate.strftime('%Y-%m-%d'))
            maturityyears = random.randint(2,3)
            maturitydays = 360 * maturityyears
            maturityDate = fromDate + datetime.timedelta(days=maturitydays+2)
            maturityDate = str(maturityDate.strftime('%Y-%m-%d'))
            notional = random.randint(30,100) * 1000000
            counterparty = self.getCounterparty(organization)
            longOrShort = random.choice(longshort)
            longOrShort = longOrShort
            
            if random.choice(pay_or_receive) == "pay":
                payccy = "USD"
                receiveccy = random.choice(ccy)
                xccy = receiveccy
            else:
                receiveccy = "USD"
                payccy = random.choice(ccy)
                xccy = payccy

            payindex = random.choice(index)
            receiveindex = random.choice(index)
            frequency = random.choice(term)
            swapvalue = 0
            message_info = {'source_stream': "swap_stream", 'id': str(id),'organization': str(organization),
                'tradedate': str(tradeDate),'settlementdate': str(settlementDate),
                'maturitydate': str(maturityDate),'notional': str(notional),
                'counterparty': str(counterparty),'longshort': str(longOrShort),
                'payccy': str(payccy),'receiveccy': str(receiveccy),
                'payindex': str(payindex),'receiveindex': str(receiveindex),
                'frequency': str(frequency),
                'valuationdate':str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                'swapvalue': str(swapvalue)}
            message_info = json.dumps(message_info)
            print(message_info)
            source_symbol2=''
            self.producer.send_messages(self.topic + "_" + xccy, source_symbol2.encode(), message_info.encode())
            if msg_cnt == 25000:
                break;
            msg_cnt += 1
            #time.sleep(.5)

if __name__ == "__main__":
    args = sys.argv
    topic = str(args[2])
    if topic == "xccy_swaps":
        ip_addr = str(args[1])
        partition_key = str(args[3])
        # create a producer object for the given ip
        prod = Producer(ip_addr,topic)
        #first produce swaps
        prod.produce_xccy_swaps(partition_key)
        #then keep generating fx rates
        prod.produce_fx_rates(partition_key)

