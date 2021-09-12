from kafka import KafkaProducer
import json
from datetime import datetime, date
from random import randrange, uniform, randint
import time

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('Error: ', exc_info=excp)

def generate_random_date(start_date, end_date):
    random_day = date.fromordinal(randint(start_date.toordinal(), end_date.toordinal()))
    return random_day.strftime("%Y-%m-%d")

def generate_msg():
    msg =  {
    "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "site_name": randrange(50),
    "posa_continent": randrange(7),
    "user_location_country": randrange(195),
    "user_location_region":	randrange(1000),
    "user_location_city": randrange(7000),
    "orig_destination_distance": round(uniform(10, 100), 2),
    "user_id": randrange(1000),
    "is_mobile": randrange(0,1),
    "is_package": randrange(0,1),
    "channel": randrange(10),
    "srch_ci": generate_random_date(date(2020, 1, 1), date.today()),
    "srch_co": generate_random_date(date(2020, 1, 1), date.today()),
    "srch_adults_cnt": randrange(9),
    "srch_children_cnt": randrange(9),
    "srch_rm_cnt": randrange(8),
    "srch_destination_id": randrange(7000),
    "srch_destination_type_id":	randrange(9),
    "is_booking": randrange(0,1),
    "cnt": randrange(269),
    "hotel_continent": randrange(7),
    "hotel_country": randrange(195),
    "hotel_market":	randrange(2117),
    "hotel_cluster": randrange(100)
    }

    return msg

if __name__ == "__main__":
    target_topic = "booking"
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                             retries=5,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for i in range(100):
        msg = generate_msg()
        producer.send(target_topic, msg).add_callback(on_send_success).add_errback(on_send_error)
        print(msg)
        time.sleep(randrange(5))

    producer.flush()
    producer.close(timeout=5)
