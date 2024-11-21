_Este README explica como correr los procesos de hosteo. La máquina EC2 pullea los contenidos de GitHub para correrlo, pero hay que hacer unas mínimas configuraciones primero._

## Instrucciones de uso

El único código que se tiene que correr es el Launcher, el resto de los archivos están embebidos en el launcher y automaticamente se utilizan.

#### Si la VM EC2 ha sido reiniciada o el repositorio de GitHub no esta hosteado correr lo siguiente

`git clone https://github.com/Don-Brownie/Trabajo-Practico-MLOPS.git`

ahi va a pedir la KEY (de GitHub) , está en el drive del equipo. Por cuestiones de seguirdad no la pongo aca. Si se hicieron un usuario, probablemente deban darse de alta su propia KEY.

Usuario: `Don-Brownie` (o cualquiera que tenga permiso en GitHub del grupo).

#### Si la VM EC2 ya tiene nuestra carpeta de GitHub adentro correr lo siguiente

vamos a la carpeta

`cd Trabajo-Practico-MLOPS/'Hosting Airflow'`

borramos lo que hay en nuestra VM de git

`git reset --hard`

hacemos un pull de la ultima versión

`git pull origin main`

Va a pedir usuario y KEY, leer el punto anterior.

## Una vez nuestro código tiene la ultima version correr lo siguiente

Viajar a la carpeta correspondiente

`cd Trabajo-Practico-MLOPS/'Hosting Airflow'`

Ejecutar el launcher

 `chmod +x Launcher`
 
  `./Launcher`

Con esto Airflow ya debería ejecutarse. Se accede mediante el IP de la EC2 y el puerto 8080
