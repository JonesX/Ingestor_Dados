import os
import threading
import numpy as np
import cv2

import gdal
import osr
import pickle

from os import listdir
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json_tricks import dump, dumps, load, loads, strip_comments
from multiprocessing.dummy import Pool as ThreadPool

def cut_send(name):
        print("Analisando imagem "+name)
        h, w, xOffSet, yOffSet = 120, 120, 60, 60
        path_imagens = "/media/administrador/SuperLeggera:0/Imagens_Habilitadas/imagens/"
        path_imagem = path_imagens+name
        imgOri = cv2.imread(path_imagem)
        ds = gdal.Open(path_imagem)
        producer = KafkaProducer(bootstrap_servers=['192.168.25.102:9092'])

        xOrigin, px_w, rot1, yOrigin, rot2, px_h = ds.GetGeoTransform()

        imagem = cv2.cvtColor(imgOri, cv2.COLOR_BGR2GRAY)

        Y = imagem.shape[0] #Altura da imagem
        X = imagem.shape[1] #Largura da imagem

        y0, y1 = 0, h
        
        while Y >= y1:
            x0, x1 = 0, w

            while X >= x1:
                imageCropped = imagem[y0:y1, x0:x1]
                
                json_imagem = {
                    "nome_arquivo":name,
                    "projection_ref":ds.GetProjectionRef(),
                    "x_origin":xOrigin,
                    "y_origin":yOrigin,
                    "px_w":px_w,
                    "px_h":px_h,
                    "X0":x0,
                    "Y0":y0,
                    "imagem":dumps(imageCropped)
                }
                
                '''
                json_imagem = {
                    "nome_arquivo":name,
                    "ds":ds.,
                    "imagem":dumps(imageCropped)
                }
                '''
                json_object = dumps(json_imagem)

                producer.send('landingqueue',bytes(json_object,'utf-8'))
                producer.flush()

                x0, x1 = (x0+xOffSet), (x1+xOffSet)

            y0, y1 = (y0+yOffSet),(y1+yOffSet)

def main():
    path_imagens = "/media/administrador/SuperLeggera:0/Imagens_Habilitadas/imagens/"
    imagens_pistas = listdir(path_imagens)

    pool = ThreadPool(1)
    results = pool.map(cut_send, imagens_pistas)
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()