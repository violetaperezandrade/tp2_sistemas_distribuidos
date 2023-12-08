# TP2 de Sistemas Distribuidos - Tolerancia a fallas

## Ejecucion mediante Docker

Se ofrecen los siguientes comandos:

- **make** <**target**>: Los target imprescindibles para iniciar y detener el sistema son docker-compose-up y docker-compose-down.

Los targets disponibles son:

- **docker-compose-scaled-up**: Inicializa el ambiente de desarrollo (buildear docker images del servidor y cliente, inicializar la red a utilizar por docker, etc.) y arranca los containers de las aplicaciones que componen el proyecto.

- **docker-compose-scaled-logs**: Muestra los logs

- **docker-compose-scaled-down**: Realiza un docker-compose stop para detener los containers asociados al compose y luego realiza un docker-compose down para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene.

## Resultados

Luego de ejecutar el sistema en cualquiera de sus versiones, se genera una carpeta **results** con los archivos _/client_C/queryX.txt_
donde X es un numero del 1 al 5 segun la consulta y C el numero de cliente

Para corroborarlos se ofrece el siguiente notebook

https://www.kaggle.com/parejafacundojose/flights-optimizer-result-comparison

Se ofrece un bash script de verificacion de resultados (prestar atencion al formato requerido)
