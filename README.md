# ETL: AdventureWorks

   ### VER LA DOCUMENTACIÓN DEL PROYECTO EN EL SIGUIENTE ENLACE PARA VER LOS **INTEGRANTES** Y LA INFORMACIÓN DEL PUNTO 4 DEL PROYECTO: [Proyecto Parte 4 ETL](https://docs.google.com/document/d/1XuoYXvlWMYJBGkiB0L7DEKg_EvLLXwmy5t-Uo7zc0Xc/edit?usp=sharing)

Este repositorio contiene la implementación de un proceso **ETL** para la construcción de un **datamart de ventas por internet y por revendedores** a partir de la base de datos **AdventureWorks2022** de Microsoft.

**Tener en cuenta que este ETL puede tardar entre 6 a 8 minutos por el proceso de traducción**

---

## Configuración del entorno

### Entorno virtual de Python

Cree y active un entorno virtual, luego instale las dependencias con:

```bash
python -m venv .venv
source .venv/bin/activate  # En Linux o macOS
.venv\Scripts\activate     # En Windows
pip install -r requirements.txt
```
### Nota importante – Usuarios de Debian/WSL (como Debian 12/13 y Ubuntu recientes):

Si aparece el error externally-managed-environment al instalar dependencias, esto ocurre por PEP 668. Para solucionarlo, recree el entorno virtual dentro del proyecto y use el pip interno:

```bash
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Verifique que el pip activo sea el interno del entorno:
```bash
which pip
```
Debe mostrar la ruta dentro de .venv/bin/pip.

### Dependencia adicional requerida

Si al ejecutar el ETL aparece un error relacionado con sentencepiece, instale manualmente:

```bash
pip install sentencepiece
```

## Montaje del sistema

La base de datos **AdventureWorks2022** ya se encuentra en la carpeta ./backups/adventure_works.slq. No es necesario que cargue otro backup.

1. Levante los contenedores con:

   ```bash
   docker compose up -d
   ```

2. ejecute para correr el ETL:

    ``` 
    python main.py
    ``` 
 
3. **!IMPORTANTE** Espere de 6 a 10 minutos mientras se realiza el ETL. Observe los logs en consola para saber en que proceso se encuetra la ejecución.

4. Abra un navegador y navege a la siguiente direccion **http://localhost:5050** y se encotrara con la pagina de **pgadmin**.
5. Escriba las siguientes credenciales para acceder:
   **Email**: admin@admin.com
   **Password**: pg123
6. click derecho en Servers y cree una base de datos con las siguientes especificaciones para poder ver la base de datos **olap** creada con el ETL, la contraseña es **pg123**:
   
   <img width="884" height="695" alt="image" src="https://github.com/user-attachments/assets/fa92ea32-24a6-4874-8bce-b07db064f92f" />
7. Abra la base de datos e ingrese al schema **dw** para ver los data marts de **fact_internet_sales** y **fact_resellers_sales**
   
8. Para detener los servicios:

   ```bash
   docker compose stop
   ```

9. Para eliminar los contenedores y volúmenes:

   ```bash
   docker compose down
   ```

10. En caso de problemas con contenedores huérfanos:

   ```bash
   docker compose down --remove-orphans
   ```

---
## RESUMEN DE LA CONEXION CON PGADMIN PARA VER LOS RESULTADOS DEL ETL.
### Conexión desde pgAdmin

Utilice los siguientes parámetros de conexión:

### Servidor OLTP

* **General → Name:** `adventure-works-oltp`
* **Connection → Host name/address:** `adventure-works-oltp`
* **Port:** `5432`
* **Username:** `postgres`
* **Password:** `pg123`

### Servidor OLAP

* **General → Name:** `adventure-works-dw`
* **Connection → Host name/address:** `adventure-works-dw`
* **Port:** `5432`
* **Username:** `postgres`
* **Password:** `pg123`
