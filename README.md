# TP1 de Sistemas Distribuidos - Middleware y Escalabilidad

## Ejecucion mediante Docker

Se ofrecen los siguientes comandos:

- **make** <**target**>: Los target imprescindibles para iniciar y detener el sistema son docker-compose-up y docker-compose-down.

Los targets disponibles son:

- **docker-compose-up**: Inicializa el ambiente de desarrollo (buildear docker images del servidor y cliente, inicializar la red a utilizar por docker, etc.) y arranca los containers de las aplicaciones que componen el proyecto.

- **docker-compose-logs**: Muestra los logs

- **docker-compose-down**: Realiza un docker-compose stop para detener los containers asociados al compose y luego realiza un docker-compose down para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene.

Los siguientes targets repiten las mismas tareas pero utilizando el archivo _docker-compose-scaled.yaml_, resultante de la ejecucion del script para escalado (**ver Escalacion**)

- **docker-compose-scaled-up**
- **docker-compose-scaled-logs**
- **docker-compose-scaled-down**

## Resultados

Luego de ejecutar el sistema en cualquiera de sus versiones, se genera una carpeta **results** con los archivos _queryX.txt_
donde X es un numero del 1 al 5 segun la consulta.

Para corroborarlos se ofrece el siguiente notebook

https://www.kaggle.com/code/parejafacundojose/flightripsoptimizer/

Ejecutarlo y descargar los archivos _kaggle_queryX_ y compararlos con los resultados obtenidos anteriormente mediante el comando

`diff <(sort ARCHIVO_OBTENIDO) <(sort ARCHIVO_GENERADO)`

## Escalacion

Se ofrece un script _scale.py_ que permite escalar facilmente los principales nodos del sistema y los reducers.  
Ejecutar como

`python3 scale.py <NUMERO_DE_NODOS> <NUMERO_DE_REDUCERS>`

con `<NUMERO_DE_NODOS>` y `<NUMERO_DE_REDUCERS>` ambos mayor que 1.

## Link al informe

https://docs.google.com/document/d/1sfDgIPvl476iK7hpkfC1Prq8Wgsqr3SpUPQEF4L8-9w/edit

## Preparacion de entorno virtual para desarrollo

```bash
sudo apt install python3.11 python3.11-venv
python3.11 -m venv venv
source venv/bin/activate
pip install pip --upgrade
pip install -r dev-requirements.txt -r requirements.txt
```

## Lint

```bash
tox
```
Verificar antes de cada push o el mismo puede fallar.
