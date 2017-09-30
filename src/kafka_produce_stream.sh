#!/bin/bash
KAFKA_PRODUCER_HOST=ip-10-0-0-5
#python produce_stream.py $KAFKA_PRODUCER_HOST fx_rates fx_rates
python produce_stream.py $KAFKA_PRODUCER_HOST xccy_swaps xccy_swaps
