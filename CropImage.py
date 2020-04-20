import os
import threading
import numpy as np
import cv2

import gdal
import osr
import pickle

from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json_tricks import dump, dumps, load, loads, strip_comments

class CropImage(threading.Thread):

    def __init__(self, path, nome_imagem):
        self._nome_imagem = nome_imagem
        self._path = path
        self._producer = KafkaProducer(bootstrap_servers=['192.168.25.102:9092'])
        self._w = 120 #largura e altura do cursor
        self._h = 120
        self._offSetX = 60 #deslocamento em x e y do cursor
        self._offSetY = 60
        threading.Thread.__init__(self)
        
    def run(self):
        path_imagem = self._path+self.nome_imagem
        imgOri = cv2.imread(path_imagem)
        ds = gdal.Open(path_imagem)

        xOrigin, px_w, rot1, yOrigin, rot2, px_h = ds.GetGeoTransform()

        imagem = cv2.cvtColor(imgOri, cv2.COLOR_BGR2GRAY)

        Y = imagem.shape[0] #Altura da imagem
        X = imagem.shape[1] #Largura da imagem

        y0, y1 = 0, self._h
        
        while Y >= y1:
            x0, x1 = 0, self._w

            while X >= x1:
                imageCropped = imagem[y0:y1, x0:x1]

                json_imagem = {
                    "x_origin":xOrigin,
                    "y_origin":yOrigin,
                    "px_w":px_w,
                    "px_h":px_h,
                    "nome_arquivo":self._nome_imagem,
                    "imagem":dumps(imageCropped)
                }

                json_object = dumps(json_imagem)

                self._producer.send('fila1',bytes(json_object,'utf-8'))
                self._producer.flush()

                x0, x1 = (x0+self._offSetX), (x1+self._offSetX)

            y0, y1 = (y0+self._offSetY),(y1+self._offSetY)