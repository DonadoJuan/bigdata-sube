from src.clases import Ubicacion
from src.clases import Coordenada

class PuntoDeCarga:

    def __init__(self, identificador, entidad, modalidad, cuit, idubicacion, nombre, tipo, calle, numero, barrio, comuna, partido, localidad, provincia, codigopostal, latitud, longitud):
        self.id = identificador
        self.entidad = entidad
        self.modalidad = modalidad
        self.cuit = cuit
        self.Ubicacion = Ubicacion(idubicacion, nombre, tipo, calle, numero, barrio, comuna, partido, localidad, provincia, codigopostal)
        self.Coordenada = Coordenada(latitud, longitud)

    id = 0
    entidad = ''
    modalidad = ''
    cuit = ''
    Ubicacion = None
    Coordenada = None
