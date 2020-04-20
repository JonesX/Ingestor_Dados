from CropImage import CropImage
from os import listdir

path_imagens = "/media/administrador/SuperLeggera:0/Imagens_Habilitadas/imagens/"
imagens_pistas = listdir(path_imagens)

cont = 0
cropers = []
for i in imagens_pistas:
    cropers.append(CropImage(path_imagens,i))
    if(cont == 2):
        break
    cont = cont + 1

for crop in cropers:
    crop.start()

