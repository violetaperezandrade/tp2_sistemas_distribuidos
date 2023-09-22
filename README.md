# TP1 de Sistemas Distribuidos - Middleware y Escalabilidad

## Preparacion de entorno virtual

```bash
sudo apt install python3.11 python3.11-venv
python3.11 -m venv venv
source venv/bin/activate
pip install pip --upgrade
pip install -r dev-requirements.txt
```

## Tests + Lint

```bash
tox
```
Verificar antes de cada push o el mismo puede fallar.

## Ejecucion mediante Docker

Se ofrecen los siguientes comandos, tomados del _TP 0_

- **make <target>**: Los target imprescindibles para iniciar y detener el sistema son docker-compose-up y docker-compose-down.

Los targets disponibles son:

- **docker-compose-up**: Inicializa el ambiente de desarrollo (buildear docker images del servidor y cliente, inicializar la red a utilizar por docker, etc.) y arranca los containers de las aplicaciones que componen el proyecto. 

- **docker-compose-down**: Realiza un docker-compose stop para detener los containers asociados al compose y luego realiza un docker-compose down para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene.

- **docker-image**: Buildea las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por docker-compose-up, por lo cual se lo puede utilizar para testear nuevos cambios en las imágenes antes de arrancar el proyecto.