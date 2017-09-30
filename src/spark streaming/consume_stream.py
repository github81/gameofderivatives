from __future__ import print_function
import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import redis
import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

#----------------------------------------------------------------
# Purpose:  this script is used to consume fx rates and swap trade messages
#           spark-submit --master spark://${SPARK_MASTER}:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 process_batch.py ${cassandra_host} ${cassandra_keyspace} ${hdfs_input_dir} ${redis_host}
#----------------------------------------------------------------

def format_dash_date(olddate):
    olddate = datetime.datetime.strptime(olddate,'%m/%d/%Y')
    dashdate = datetime.date(olddate.year,olddate.month,olddate.day).strftime('%Y-%m-%d')
    return dashdate

def format_slash_date(olddate):
    olddate = datetime.datetime.strptime(olddate,'%Y-%m-%d')
    slash_date = datetime.date(olddate.year,olddate.month,olddate.day).strftime('%m/%d/%Y')
    return slash_date

def get_redis_db(db_type):
    switcher = {
        "EUR": 8,
        "JPY": 9,
        "GBP": 10,
        "AUD": 11,
        "CHF": 12,
        "XAG": 13,
        "XAU": 14,
        "NZD": 15,
        "ir_rates": 0,
        #current fx rates
        "fx_rates": 1,
        #all (current + historical)
        "all_fx_rates": 2,
    }
    db_num = switcher.get(db_type,"nothing")
    return db_num

def write_to_redis(key,value,db_type):
    db_num = get_redis_db(db_type)
    db = redis.StrictRedis(host='<public-dns>', port=6379, db=db_num)
    db.set(key,value)

def read_from_redis(key,db_type):
    rate = 0
    db_num = get_redis_db(db_type)
    db = redis.StrictRedis(host='<public-dns>', port=6379, db=db_num)
    value = db.get(key)
    return value

def read_keys_from_redis(db_type):
    rate = 0
    db_num = get_redis_db(db_type)
    db = redis.StrictRedis(host='<public-dns>', port=6379, db=db_num)
    keys = db.keys()
    return keys

def write_rdd_pipeline_to_redis(pipelined_rdd_data,redis_host,redis_port,db_type):
    rdd_data = pipelined_rdd_data.map(lambda x: x.encode("ascii", "ignore").split("\t")).map(lambda x: ((str(x[0])+"-"+str(x[1])+"-"+str(x[2])),str(x[3])))
    if db_type == "ir_rates":
        rdd_data.foreach(lambda x: write_to_redis(x[0],x[1],"ir_rates"))
    if db_type == "fx_rates":
        rdd_data.foreach(lambda x: write_to_redis(x[0],x[1],"fx_rates"))

def get_ir_rate(date,days,frequency):
    rate_key = str(days) + "-LIBOR-" + str(frequency)
    rate = read_from_redis(rate_key,"ir_rates")
    return float(rate)

def get_fx_rate(date,ccy):
    rate_key = str(date) + "-USD-" + str(ccy)
    rate = read_from_redis(rate_key,"fx_rates")
    return float(rate)

def calc_number_of_months(frequency):
    switcher = {
        "3M": 3,
        "6M": 6,
        "1Y": 12,
    }
    number_of_months = switcher.get(frequency,"nothing")
    return number_of_months

def calc_number_of_days(frequency):
    switcher = {
        "3M": 90,
        "6M": 180,
        "1Y": 360,
    }
    number_of_days = switcher.get(frequency,"nothing")
    return number_of_days

def calc_number_of_periods(settlement_date,maturity_date,number_of_days):
    start_date = datetime.datetime.strptime(settlement_date, '%m/%d/%Y')
    end_date = datetime.datetime.strptime(maturity_date, '%m/%d/%Y')
    term = ((end_date-start_date).days)/number_of_days
    number_of_periods = int(term)
    return number_of_periods
    
def calc_payment_dates(settlement_date,number_of_days,number_of_periods):
    start_date = datetime.datetime.strptime(settlement_date,'%m/%d/%Y')
    date_intervals = []
    for i_period in range(0,number_of_periods):
        start_date = start_date + datetime.timedelta(days=number_of_days)
        start_date_str = datetime.date(start_date.year,start_date.month,start_date.day).strftime('%-m/%-d/%Y')
        date_intervals.append(start_date_str)
        start_date = start_date + datetime.timedelta(days=number_of_days)
    return date_intervals

def calc_fixed_rate(as_of_date,number_of_days,number_of_periods,ccy):
    sum_of_days = 0
    discount_factor = 0
    total_discount_factor = 0
    last_discount_factor = 0
    for i_period in range(0,number_of_periods):
        sum_of_days += number_of_days
        interest_rate = get_ir_rate(as_of_date,str(sum_of_days),ccy)
        #calculate discount factor
        discount_factor = 1/(1+(float(interest_rate)*(sum_of_days/360)))
        total_discount_factor += discount_factor
        last_discount_factor = discount_factor
    #calculate the fixed rate
    fixed_rate = (1-last_discount_factor)/total_discount_factor
    return fixed_rate

def calc_pv_fixed_payments(as_of_date,settlement_date,number_of_days,number_of_periods,frequency):
    #calculate the fixed rate first
    fixed_rate = calc_fixed_rate(as_of_date,number_of_days,number_of_periods,frequency)
    start_date = datetime.datetime.strptime(settlement_date, '%m/%d/%Y')
    pv_date = datetime.datetime.strptime(as_of_date, '%m/%d/%Y')
    days_from_settlement = (pv_date - start_date).days
    sum_of_days = number_of_days - days_from_settlement
    discount_factor = 0
    total_discount_factor = 0
    last_discount_factor = 0
    for i_period in range(0,number_of_periods):
        if sum_of_days < 0:
            sum_of_days += number_of_days
            continue
        interest_rate = get_ir_rate(as_of_date,str(sum_of_days),frequency)
        discount_factor = 1/(1+(float(interest_rate)*(sum_of_days/360)))
        total_discount_factor += discount_factor
        last_discount_factor = discount_factor
        sum_of_days += number_of_days
    #calculate the pv of fixed payments
    pv_fixed_payments = (fixed_rate*total_discount_factor)+(1+last_discount_factor)
    return pv_fixed_payments

def get_reset_date(as_of_date,number_of_days,payment_dates):
    pv_date = datetime.datetime.strptime(as_of_date, '%m/%d/%Y')
    period_start_date = ''
    for payment_date in payment_dates:
        period_start_date = payment_date
        reset_date = datetime.datetime.strptime(payment_date, '%m/%d/%Y')
        days_from_period_start = (pv_date-reset_date).days
        if days_from_period_start < number_of_days:
            break
    #return the reset date
    return period_start_date

def calc_pv_float_payments(as_of_date,number_of_days,number_of_periods,payment_dates,frequency):
    #get reset date
    reset_date = get_reset_date(as_of_date,number_of_days,payment_dates)
    #get reset rate
    reset_rate = get_ir_rate(reset_date,str(number_of_days),frequency)
    #get the interest rate (reset rate) set at the beginning of the period
    start_date = datetime.datetime.strptime(reset_date, '%m/%d/%Y')
    pv_date = datetime.datetime.strptime(as_of_date, '%m/%d/%Y')
    days_from_settlement = (pv_date - start_date).days
    sum_of_days = number_of_days - days_from_settlement
    for i_period in range(0,number_of_periods):
        if sum_of_days < 0:
            sum_of_days += number_of_days
            continue
        else:
            break
    interest_rate = get_ir_rate(as_of_date,str(sum_of_days),frequency)
    #calculate pv of float payments
    pv_float_payments = (1+reset_rate)*float(interest_rate)
    return pv_float_payments

def convert_value(as_of_date,value,ccy):
    conversion_rate = get_fx_rate(as_of_date,ccy)
    converted_value = value/float(conversion_rate)
    return converted_value

#function to value the legs of a swap contract
#convert the value to USD if required
def calc_swap_value(as_of_date,settlement_date,number_of_days,number_of_periods,payment_dates,frequency,pay_index,receive_index,pay_ccy,receive_ccy,notional,long_short):
    #calculate PV
    pay_fixed_payments = 0
    pay_float_payments = 0
    if pay_index == "FIXED":
        pay_fixed_payments = calc_pv_fixed_payments(as_of_date,settlement_date,number_of_days,number_of_periods,frequency)
    if pay_index == "LIBOR":
        pay_float_payments = calc_pv_float_payments(as_of_date,number_of_days,number_of_periods,payment_dates,frequency)
    #calculate PV
    receive_fixed_payments = 0
    receive_float_payments = 0
    if receive_index == "FIXED":
        receive_fixed_payments = calc_pv_fixed_payments(as_of_date,settlement_date,number_of_days,number_of_periods,frequency)
    if receive_index == "LIBOR":
        receive_float_payments = calc_pv_float_payments(as_of_date,number_of_days,number_of_periods,payment_dates,frequency)
    #convert the pv payments if it is a cross currency swap
    if pay_ccy != "USD":
        pay_fixed_payments = convert_value(as_of_date,pay_fixed_payments,pay_ccy)
        pay_float_payments = convert_value(as_of_date,pay_float_payments,pay_ccy)
    if receive_ccy != "USD":
        receive_fixed_payments = convert_value(as_of_date,receive_fixed_payments,receive_ccy)
        receive_float_payments = convert_value(as_of_date,receive_float_payments,receive_ccy)
    #calculate the swap value
    swap_value = 0;
    if long_short == "LONG" and receive_index == "FIXED" and pay_index == "LIBOR":
        swap_value = receive_fixed_payments - pay_float_payments
    elif long_short == "LONG" and receive_index == "LIBOR" and pay_index == "FIXED":
        swap_value = receive_float_payments - pay_fixed_payments
    elif long_short == "SHORT" and receive_index == "LIBOR" and pay_index == "FIXED":
        swap_value = -1*(receive_float_payments - pay_fixed_payments)
    elif long_short == "SHORT" and receive_index == "FIXED" and pay_index == "LIBOR":
        swap_value = -1*(receive_fixed_payments - pay_float_payments)
    elif long_short == "SHORT" and receive_index == "FIXED" and pay_index == "FIXED":
        swap_value = -1*(receive_fixed_payments - pay_fixed_payments)
    elif long_short == "LONG" and receive_index == "FIXED" and pay_index == "FIXED":
        swap_value = receive_fixed_payments - pay_fixed_payments
    swap_value *= notional
    return swap_value

#function to value a swap contract
def process_swap_json(json_message):
    record = json_message
    rec = json.loads(record)
    swap_value=0
    id = rec['id']
    organization = rec['organization']
    trade_date = rec['tradedate']
    settlement_date = rec['settlementdate']
    maturity_date = rec['maturitydate']
    notional = float(rec['notional'])
    counterparty = rec['counterparty']
    pay_ccy = rec['payccy']
    receive_ccy = rec['receiveccy']
    pay_index = rec['payindex']
    receive_index = rec['receiveindex']
    long_short = rec['longshort']
    frequency = rec['frequency']
            
    #calculate the swap value
    todays_date = datetime.datetime.now().strftime("%-m/%-d/%Y")
    settlement_date = format_slash_date(settlement_date)
    maturity_date = format_slash_date(maturity_date)
    #get the basic details required to price the swaps
    number_of_months = calc_number_of_months(frequency)
    number_of_days = calc_number_of_days(frequency)
    number_of_periods = calc_number_of_periods(settlement_date,maturity_date,number_of_days)
    payment_dates = calc_payment_dates(settlement_date,number_of_days,number_of_periods)
    if not payment_dates:
        return ""
    swap_value=calc_swap_value(todays_date,settlement_date,number_of_days,number_of_periods,payment_dates,frequency,pay_index,receive_index,pay_ccy,receive_ccy,notional,long_short)
    #change the dates back to dash format
    settlement_date = format_dash_date(settlement_date)
    maturity_date = format_dash_date(maturity_date)
    #make the json message to be inserted
    message_info = {'source_stream': "swap_stream",'id': str(id),'organization': str(organization),
        'tradedate': str(trade_date),'settlementdate': str(settlement_date),
        'maturitydate': str(maturity_date),'notional': str(notional),
        'counterparty': str(counterparty),'longshort': str(long_short),
        'payccy': str(pay_ccy),'receiveccy': str(receive_ccy),
        'payindex': str(pay_index),'receiveindex': str(receive_index),
        'frequency': str(frequency),'valuationdate':str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        'swapvalue': str(swap_value)}
    return message_info

#function to process swap contracts based on the currency provided
#swap contracts are read from redis database
def reprocess_xccy_swaps(price_ccy):
    #stream the processed back on kafka
    client = SimpleClient("ip-10-0-0-6")
    producer = KeyedProducer(client)
    topic = "processed_xccy_swaps"
    #get all the KEYS saved by price_ccy db
    swaps = read_keys_from_redis(price_ccy)
    for swap_id in swaps:
        print(swap_id + "," + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")))
        swap_record = read_from_redis(swap_id,price_ccy)
        if swap_record == "\"no payment dates\"":
            continue
        message_info = process_swap_json(swap_record)
        if message_info == "":
            continue
        message_info = json.dumps(message_info)
        source_symbol3=''
        producer.send_messages(topic + "_" + price_ccy, source_symbol3.encode(), message_info.encode())

#function to process a swap rdd partition and save to redis by currency
def process_xccy_swaps(iter):
    #stream the processed back on kafka
    #client = SimpleClient("ip-10-0-0-6")
    #producer = KeyedProducer(client)
    #topic = "processed_xccy_swaps"
    for record in iter:
        rec = json.loads(record)
        if rec['source_stream'] == "fx_stream":
            price_ccy = rec['price']
            rate = rec['rate']
            date = rec['date']
            #write current fx rates to redis
            key=str(date) + "-USD-" + str(price_ccy)
            write_to_redis(key,rate,"fx_rates")
            #reprocess the swaps stored in redis
            reprocess_xccy_swaps(price_ccy)
            continue
        #store the swaps in redis by currency
        id = rec['id']
        organization = rec['organization']
        trade_date = rec['tradedate']
        settlement_date = rec['settlementdate']
        maturity_date = rec['maturitydate']
        notional = float(rec['notional'])
        counterparty = rec['counterparty']
        pay_ccy = rec['payccy']
        receive_ccy = rec['receiveccy']
        pay_index = rec['payindex']
        receive_index = rec['receiveindex']
        long_short = rec['longshort']
        frequency = rec['frequency']
        valuationdate = rec['valuationdate']
        swapvalue = rec['swapvalue']
        message_info = {'source_stream': "swap_stream",'id': str(id),'organization': str(organization),
        'tradedate': str(trade_date),'settlementdate': str(settlement_date),
        'maturitydate': str(maturity_date),'notional': str(notional),
        'counterparty': str(counterparty),'longshort': str(long_short),
        'payccy': str(pay_ccy),'receiveccy': str(receive_ccy),
        'payindex': str(pay_index),'receiveindex': str(receive_index),
        'frequency': str(frequency),'valuationdate':str(valuationdate),
        'swapvalue': str(swapvalue)}
        message_info = json.dumps(message_info)
        #source_symbol2=''
        #producer.send_messages(topic, source_symbol2.encode(), message_info.encode())
        xccy = receive_ccy
        if pay_ccy != "USD":
            xccy = pay_ccy
        write_to_redis(str(id),message_info,xccy)

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#function to write a rdd partition to cassandra
def write_processed_swaps_to_cdb(rdd):

    try:
        #write processed swaps to cassandra db
        spark = getSparkSessionInstance(rdd.context.getConf())
        df = spark.read.json(rdd)
        load_options = {"table": "swap", "keyspace": "swappg"}
        df.write \
            .format("org.apache.spark.sql.cassandra") \
                .options(**load_options) \
                    .mode('append') \
                        .save()
    except:
        pass

def main(argv):

    kafka_producer = argv[0]
    hdfs_input_dir = argv[3]
    topic = argv[2]
    redis_host = argv[1]
    redis_port = 6379
    msg_count=0
    db_num = 0
    fx_topic="fx_rates"
    ir_topic="ir_rates"
    swap_topic="xccy_swaps"
    processed_swaps_topic="processed_xccy_swaps"

    app_name = argv[4]
    #spark context
    spark_context = SparkContext(appName=app_name)
    spark_context.setLogLevel("WARN")
    #streaming context
    streaming_context = StreamingContext(spark_context, 10)
    kafka_dns = kafka_producer + ":9092"

    #load ir rates to redis
    #redis db=0 is for fx rates and ir rates
    term = ["3M","6M","1Y"]
    for iterm in term:
        ir_file = "ir_"+iterm+".txt"
        pipelined_rdd_ir_data = spark_context.textFile(hdfs_input_dir + "/ir_data/"+ir_file)
        write_rdd_pipeline_to_redis(pipelined_rdd_ir_data,redis_host,redis_port,"ir_rates")
    #also get fx rates
    fx_file = "fx_USD.txt"
    pipelined_rdd_fx_data = spark_context.textFile(hdfs_input_dir + "/fx_data/"+fx_file)
    write_rdd_pipeline_to_redis(pipelined_rdd_fx_data,redis_host,redis_port,"fx_rates")

    #list of topics
    xccy_swap_topic_list = [topic + "_EUR", topic + "_JPY", topic + "_GBP", topic + "_AUD", topic + "_CHF", topic + "_XAG", topic + "_XAU", topic + "_NZD"]
    if topic == swap_topic:
        try:
            kafka_stream = KafkaUtils.createDirectStream(streaming_context,xccy_swap_topic_list,{"bootstrap.servers": kafka_dns})
            swap_lines = kafka_stream.map(lambda x: x[1])
            swap_lines.count().map(lambda x:'swap Messages in this batch: %s' % x).pprint()
            swap_lines.foreachRDD(lambda rdd: rdd.foreachPartition(lambda x: process_xccy_swaps(x)))
        except:
            pass
    if topic == processed_swaps_topic:
        try:
            kafka_stream = KafkaUtils.createDirectStream(streaming_context,xccy_swap_topic_list,{"bootstrap.servers": kafka_dns})
            processed_swap_lines = kafka_stream.map(lambda x: x[1])
            processed_swap_lines.count().map(lambda x:'processed swap Messages in this batch: %s' % x).pprint()
            processed_swap_lines.foreachRDD(lambda rdd: write_processed_swaps_to_cdb(rdd))
        except:
                pass

    streaming_context.start()
    streaming_context.awaitTermination()


if __name__=="__main__":
    main(sys.argv[1:])

