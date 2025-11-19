# ETL: AdventureWorks

Este repositorio contiene la implementación de un proceso **ETL** para la construcción de un **datamart de ventas por internet y por revendedores** a partir de la base de datos **AdventureWorks2022** de Microsoft.

**Tener en cuenta que este ETL puede tardar entre 6 a 8 minutos por el proceso de traducción**

---

## Configuración del entorno

### Entorno virtual de Python

Cree y active un entorno virtual, luego instale las dependencias con:

```bash
python -m venv venv
source venv/bin/activate  # En Linux o macOS
venv\Scripts\activate     # En Windows
pip install -r requirements.txt
```

### Entorno Nix (opcional)

Si se encuentra en un entorno **Nix**, puede levantar el shell de desarrollo encapsulado mediante:

```bash
nix develop
```

Esto cargará automáticamente todas las dependencias definidas en el flake.

---

## Montaje del sistema

1. Descargue la base de datos **AdventureWorks2022 OLAP** en **formato PostgreSQL**.
   (Debe asegurarse de utilizar una versión compatible con PostgreSQL).

2. Coloque el archivo descargado en la carpeta `backups/`.

3. Levante los contenedores con:

   ```bash
   docker compose up -d
   ```

4. Para detener los servicios:

   ```bash
   docker compose stop
   ```

5. Para eliminar los contenedores y volúmenes:

   ```bash
   docker compose down
   ```

6. En caso de problemas con contenedores huérfanos:

   ```bash
   docker compose down --remove-orphans
   ```

---

## Conexión desde pgAdmin

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
