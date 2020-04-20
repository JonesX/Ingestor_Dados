import numpy as np
import cv2
import json

import gdal
import osr
import pickle

from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json_tricks import dump, dumps, load, loads, strip_comments

path_imagens = "/media/administrador/SuperLeggera:0/Imagens_Habilitadas/imagens"

classificador = cv2.CascadeClassifier("cascade_50_15_haar.xml")

def calc_coordinates(ds, px, py):
    xoffset, px_w, rot1, yoffset, rot2, px_h = ds.GetGeoTransform()
    print(xoffset, px_w, rot1, yoffset, rot2, px_h)
    mX = px * px_w + xoffset
    mY = py * px_h + yoffset

    crs = osr.SpatialReference()
    crs.ImportFromWkt(ds.GetProjectionRef())

    crsGeo = osr.SpatialReference()
    crsGeo.ImportFromEPSG(4326)
    t = osr.CoordinateTransformation(crs, crsGeo)
    coords = t.TransformPoint(mX, mY)

    coordinates = {
        "latitude":coords[1],
        "longitude":coords[0]
    }

    print(coordinates)

    return coordinates


def calc_histogram(img):
    hist = dict()
    x = img.shape[1]
    y = img.shape[0]

    for i in range(0,x):
        for j in range(0,y):
            if img[i,j] not in hist:
                hist[img[i,j]] = 1
            else:
                hist[img[i,j]] += 1
    return hist


def check_above_70(img, hist):
    total = img.shape[0]*img.shape[1]
    for key in sorted(hist):
        percent = hist[key]/total
        if percent >= 0.7:
            return True
    return False


def detectar_pista(img):
    faces = classificador.detectMultiScale(img, 1.3, 5, 1, (60, 60))
    for (x0, y0, w, h) in faces:
        print(x0, y0, w, h)

    print(type(faces))
    if(len(faces) > 0):
        return True
    else:
        return False


producer = KafkaProducer(bootstrap_servers=['192.168.25.102:9092'])

w, h = 120, 120 #largura e altura da imagem
#w, h = 240, 240 #largura e altura da imagem

offSetX, offSetY = 60, 60 #deslocamento em x e y

#path_image = "Imagens/imagem_teste.tif"
path_image = "Imagens/CBERS_4_PAN5M_20150623_169_107_L4_BAND1.tif"

imgOri = cv2.imread(path_image)
ds = gdal.Open(path_image)

imagem = cv2.cvtColor(imgOri, cv2.COLOR_BGR2GRAY)

Y = imagem.shape[0] #Altura da imagem
X = imagem.shape[1] #Largura da imagem

y0, y1 = 0, h

parar = False
while Y >= y1:
    x0, x1 = 0, w
    while X >= x1:

        imageCropped = imagem[y0:y1, x0:x1]

        hist = calc_histogram(imageCropped)

        if(not check_above_70(imageCropped, hist)):
            #if(detectar_pista(imageCropped)):
            aux = "-----------------------------------------------------------"
            print(aux)

            aux = "x0="+str(x0)+" y0="+str(y0)+" x1="+str(x1)+" y1="+str(y1)
            print(aux)

            pX = x0+((x1-x0)/2)
            pY = y0+((y1-y0)/2)

            aux = str(pX)+" "+str(pY)
            coordinate = calc_coordinates(ds, pX, pY)

            json_imagem = {
                "nome_arquivo":path_image,
                "coordenadas":coordinate,
                "imagem":dumps(imageCropped)
            }

            json_object = dumps(json_imagem)

            #producer.send('fila1',bytes(json_object,'utf-8'))
            #producer.flush()
            
        cv2.imshow("PRODUTOR", imageCropped)

        x0, x1 = (x0+offSetX), (x1+offSetX)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            #arqLog.close()
            parar = True
            break

    y0, y1 = (y0+offSetY),(y1+offSetY)
    if parar:
        break

cv2.destroyAllWindows()
