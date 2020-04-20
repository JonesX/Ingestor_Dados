from kafka import KafkaConsumer
from json_tricks import dump, dumps, load, loads, strip_comments
import cv2
import numpy as np

#consumer = KafkaConsumer('tccpuc', bootstrap_servers='192.168.25.102:9092', auto_offset_reset='earliest')
consumer = KafkaConsumer('analysequeue', bootstrap_servers='192.168.25.102:9092')

for message in consumer:
    message = message.value
    json_value = message.decode("utf-8")
    print(json_value)
    '''
    json_value = message.decode("utf-8")
    json_value = loads(json_value)
    json_image = dict(json_value)
    

    print("Nome: ", json_image['nome_arquivo'])
    print("Latitude: ", json_image["coordenadas"]["latitude"])
    print("Longitude: ", json_image["coordenadas"]["longitude"])
    
    imagem = loads(json_image["imagem"])
    #print(type(imagem))
    cv2.imshow("CONSUMIDOR", imagem)

    if cv2.waitKey(1) & 0xFF == ord("q"):
            parar = True
            break

cv2.destroyAllWindows()]
'''