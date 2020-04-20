from kafka import KafkaProducer
from kafka.errors import KafkaError
from json_tricks import dump, dumps, load, loads, strip_comments

producer = KafkaProducer(bootstrap_servers=['192.168.25.102:9092'])
'''
json_imagem = {
    "nome_imagem":"CBERS_4_PAN5M_20150909_169_108_L2_BAND1",
    "latitude":-7.3177,
    "longitude":-55.8552
}
'''
json_imagem = {
    "nome_imagem":"CBERS_4_PAN5M_20150623_169_107_L4_BAND1",
    "latitude":-6.6500,
    "longitude":-56.7134
}

json_object = dumps(json_imagem)

producer.send('mapqueue',bytes(json_object,'utf-8'))
producer.flush()