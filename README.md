# Demo Quind en la nube de Azure

[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)

Date: 07/08/2023

### Contenido

- [Lo que construiremos.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
    1. [Detalles de implementación.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
    2. [Recursos usados y explicación.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
    3. [Video explicativo.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [Creación de la infraestructura a usar en Azure.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [Puesta en marcha del broker de Kafka.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [De local a Kafka.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [Implementando lo necesario en Databricks.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [Generando vistas en Synapse Analytics.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
    - [Código implementado.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
        1. [Creando la base de datos.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
        2. [Creando las vistas.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)
- [Lo que construimos.](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21)

### Enlaces externos

<aside>
<img src="https://cdn.iconscout.com/icon/free/png-256/free-azure-devops-3628645-3029870.png?f=webp" alt="https://cdn.iconscout.com/icon/free/png-256/free-azure-devops-3628645-3029870.png?f=webp" width="15px" /> <a href="https://drive.google.com/file/d/14yFy4zCQs3M6ZA1pN64GY_o59eU-S-qF/view?usp=drive_link">Repositorio Azure DevOps.</a>.

</aside>

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Octicons-mark-github.svg/2048px-Octicons-mark-github.svg.png" alt="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Octicons-mark-github.svg/2048px-Octicons-mark-github.svg.png" width="15px" /> <a href="https://github.com/sapuertaf/KafkaDataBricksnAzure">Repositorio GitHub.</a>.

</aside>

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Google_Drive_icon_%282020%29.svg/2295px-Google_Drive_icon_%282020%29.svg.png" alt="https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Google_Drive_icon_%282020%29.svg/2295px-Google_Drive_icon_%282020%29.svg.png" width="15px" /> <a href="https://drive.google.com/file/d/1veEk0QK3O8SHwAqAZbxcvc5DRmtzNvXt/view?usp=sharing">Modelo C4: Diagramas de arquitectura.</a>.

</aside>

### Descargas

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/Diagrams.net_Logo.svg/2048px-Diagrams.net_Logo.svg.png" alt="https://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/Diagrams.net_Logo.svg/2048px-Diagrams.net_Logo.svg.png" width="15px" /> Modelo C4: Diagramas de arquitectura.

[file](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/Modelo-C4-Diagrama-de-arquitectura.drawio)

</aside>

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/c/c6/.csv_icon.svg/1200px-.csv_icon.svg.png" alt="https://upload.wikimedia.org/wikipedia/commons/thumb/c/c6/.csv_icon.svg/1200px-.csv_icon.svg.png" width="15px" /> CSV’s requeridos para la ejecución.

[data.zip](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/data.zip)

</aside>

<aside>
<img src="https://cdn-icons-png.flaticon.com/512/1636/1636540.png" alt="https://cdn-icons-png.flaticon.com/512/1636/1636540.png" width="15px" /> Lo que construiremos.

</aside>

El presente documento contiene en detalle la implementación de un proyecto de analítica completo haciendo uso de la arquitectura Data Lake House.

![Diagrama de contexto del sistema.](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-contexto-del-sistema.svg)

Diagrama de contexto del sistema.

### 1. Detalles de implementación

Principalmente se hará uso de Apache Kafka para el consumo de los datos de topicos definidos y Azure para el despliegue de la infraestructura LakeHouse.

### 2. Recursos usados y explicación

A continuación, se explica brevemente la infraestructura creada y a ser usada:

- **Máquina Virtual:**
- **Espacio de trabajo de Databricks:** Utilizado para la ejecución de jobs de PySpark en tiempo real para la ingesta, procesamiento y carga de datos. En este espacio de trabajo se crean notebooks (scripts) y clusters para la ejecución de trabajos.
- **Alcance secreto Databricks:** Utilizado para la autenticación con la cuenta de almacenamiento para habilitar la escritura y lectura en el Data Lake desde Databricks.
- **Cuenta de almacenamiento:** Utilizada para la creación del Data Lake. En esta se crean contenedores utilizados para almacenar objetos de datos en carpetas. Cada carpeta representa una tabla.
- **Key Vault (bóveda de llaves):** Utilizada para almacenar la clave primaria de la cuenta de almacenamiento.
- **Azure Synapse Analytics:** Utilizado para la generación de vistas materializadas que permiten vincular los datos almacenados en el Data Lake. Además, puede ser utilizado para la generación de consultas ad-hoc haciendo uso de lenguaje SQL.

### 3. Video explicativo

En este video se detalla la creación de la infraestructura para la implementación del proyecto recientemente descrito. 

[https://www.notion.so](https://www.notion.so)

---

<aside>
<img src="https://swimburger.net/media/ppnn3pcl/azure.png" alt="https://swimburger.net/media/ppnn3pcl/azure.png" width="20px" /> Creación de la infraestructura a usar en Azure

</aside>

Primeramente deberemos de levantar la infraestructura especificada en la sección: Lo que construiremos - Recursos usados y explicación.

![Diagrama de despliegue: Infraestructura de Azure usada. ](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-despliegue-Infraestructura-Azure.svg)

Diagrama de despliegue: Infraestructura de Azure usada. 

---

<aside>
<img src="https://cdn-icons-png.flaticon.com/512/6150/6150708.png" alt="https://cdn-icons-png.flaticon.com/512/6150/6150708.png" width="20px" /> Puesta en marcha del broker de Kafka

</aside>

Una vez creada la infraestructura en Azure pondremos en marcha el broker de Kafka con sus respectivos tópicos.

Para esto accederemos a la máquina virtual creada anteriormente y crearemos los archivos de configuración de Kafka.

---

<aside>
<img src="https://icons.iconarchive.com/icons/pictogrammers/material/512/apache-kafka-icon.png" alt="https://icons.iconarchive.com/icons/pictogrammers/material/512/apache-kafka-icon.png" width="20px" /> De local a Kafka

</aside>

Para poder tener datos que consumir posteriormente en Databricks, primero debemos de enviar datos desde nuestro computador hasta los tópicos del broker de Kafka (una vez este esté activo).

![Diagrama de componentes: Local a Kafka](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-componentes-Local-Kafka.svg)

Diagrama de componentes: Local a Kafka

Mientras que en el video presentado en la sección: [Creación de la infraestructura a usar en Azure - Video explicativo](https://www.notion.so/Demo-Quind-en-la-nube-de-Azure-fc299097bd3e45ea936c2f0f2ccc1fa7?pvs=21), se hace uso de Apache NiFi para la lectura y envío de los datos; en este caso en cuestión se hara uso de código Python para la implementación del productor.

Los siguientes son los archivos CSV requeridos para la ejecución:

[sales.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/sales.csv)

[sales_order.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/sales_order.csv)

[sales_territory.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/sales_territory.csv)

[reseller.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/reseller.csv)

[product.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/product.csv)

[date.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/date.csv)

[customer.csv](Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/customer.csv)

---

<aside>
<img src="https://avatars.githubusercontent.com/u/4998052?s=280&v=4" alt="Demo%20Quind%20en%20la%20nube%20de%20Azure%20fc299097bd3e45ea936c2f0f2ccc1fa7/62c719e9b44be1961554a6e0.png" width="20px" /> Implementando lo necesario en Databricks

</aside>

Leeremos los datos disponibles en los topicos de Kafka para llevarlos a la cuenta de almacenamiento de Azure. 

![Diagrama de componentes: Kafka - Databricks y Storage Account.](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-componentes-Kafka-Databricks-Storage%20Account.svg)

Diagrama de componentes: Kafka - Databricks y Storage Account.

---

<aside>
<img src="https://seeklogo.com/images/A/azure-synapse-analytics-logo-B87A556A9C-seeklogo.com.png" alt="https://seeklogo.com/images/A/azure-synapse-analytics-logo-B87A556A9C-seeklogo.com.png" width="20px" /> Generando vistas en Synapse Analytics

</aside>

Una vez los datos son almacenados en la cuenta de almacenamiento crearemos vistas haciendo uso de Synapse Analytics para vincular y consultar los datos almacenados en el Data Lake.

![Diagrama de componentes: Storage Account y Synapse Analytics.](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-componentes-StorageAccount-SynapseAnalytics.svg)

Diagrama de componentes: Storage Account y Synapse Analytics.

### Código implementado

### 1. Creando la base de datos

```sql
//Creamos la base de datos donde se van a alojar las vistas.
CREATE DATABASE DataBaseName;
```

### 2. Creando las vistas

```sql
//Creamos la vista vinculando los datos almacenados en un contenedor de la cuenta de almacenamiento.
CREATE VIEW ViewName AS
SELECT *
FROM OPENROWSET(
	BULK 'https://storageaccountname.dfs.core.windows.net/containername/pathtodeltafolder',
	FORMAT = 'DELTA',
	HEADER_ROW = TRUE
) AS ViewData
```

---

<aside>
<img src="https://cdn-icons-png.flaticon.com/512/957/957636.png" alt="https://cdn-icons-png.flaticon.com/512/957/957636.png" width="20px" /> Lo que construimos

</aside>

Finalmente, el siguiente diagrama detalla lo construido.

![Diagrama de componentes: Todo el sistema.](https://raw.githubusercontent.com/sapuertaf/KafkaDataBricksnAzure/c937c9561fb3e8c4165e6e774ca477b04a36ec11/resources/img/Diagrama-componentes-Todo-sistema.svg)

Diagrama de componentes: Todo el sistema.