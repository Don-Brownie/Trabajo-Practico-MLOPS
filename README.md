_Este archivo corre todos los procesos de hosteo. La máquina EC2 pullea los contenidos de aquí para correrlo._

### Instrucciones de uso.

El único código que se tiene que correr es el Launcher, el resto de los archivos están embebidos en el launcher y automaticamente se utilizan.

### Si la VM EC2 ha sido reiniciada o el repositorio de GitHub no esta hosteado correr lo siguiente

`git clone https://github.com/Don-Brownie/Trabajo-Practico-MLOPS.git`

ahi va a pedir la KEY, está en el drive del equipo. Por cuestiones de seguirdad no la pongo aca.

Usuario: `Don-Brownie` (o cualquiera que tenga permiso en GitHub del grupo).

### Con git ya clonado en la EC2

Viajar a la carpeta correspondiente

`cd Trabajo-Practico-MLOPS/Hosting Airflow`

Ejecutar el launcher

 `chmod +x Launcher`
 
  `./Launcher`

Con esto Airflow ya debería ejecutarse. Se accede mediante el IP de la EC2 y el puerto 8080
