/*
 * kernel.c
 *
 *  Created on: 4 abr. 2019
 *      Author: utnso
 */

#include "LissandraFileSystem.h"
#include "lfsComunicacion.h"

int tamanioTotalTabla = 0;
char *separador2 = "\n";
char *separator = " ";
const char* comandosPermitidos[] = { "select", "insert", "create", "describe",
		"drop", "journal", "add", "run", "metrics", "salir"

};

int cantidad_de_dumps = 0;
int dumps_a_dividir =1;

char* tablaAverificar; // directorio de la tabla
char* path_tabla_metadata;
char* archivoParticion;
char* registroPorAgregar;
int primerVoidEsRegistro = 1;

/*
 * REQUERIMIENTOS:
 *  - ������ verificarExistenciaTabla    () ? Nota: est������ hecho el m������todo de verificarExistenciaTabla()
 *  - crearTabla(nombre, tipoConsistencia, nroParticiones, compactationTime)  // e.g.: CREATE TABLA1 SC 4 60000
 *  - describe(nombre)
 *  - bool      :verificarExistenciaTabla(nombre)
 *  - obtenerMetadata(nombre)                           // ver que hacer ac������
 *  - crearMemtable()
 *  - << todo lo necesario para gestionar las memTables >>
 *  - registro**:escanearTabla    (nombre,key)          // retorna un array de punterosa registros.
 *              :escaneaBinario   (key)
 *              :escaneaTemp      (key)
 *              :escaneaTempc     (key)
 *  - registro**:escanearMemtable (key)                 // retorna un array de punterosa registros.
 *
 *  NOTA: nombres de tablas no se distingue uppercase de lowercase. Doesn't do difference by cases.
 */
int main() {

	pantallaLimpiar();

	sem_init(&semaforoQueries, 0, 1);
	mutexIniciar(&memtable_mx);
	mutexIniciar(&listaTablasInsertadas_mx);
	//mutexIniciar(&semaforo); MERGE

	list_queries = list_create();

	LisandraSetUP(); // CONFIGURACION Y SETEO SOCKET

	cargarBitmap();
	int socketLFS = iniciar_servidor("127.0.0.1", "8010"); // AGREGAR IP A ARCHIV CONFIG
	if (socketLFS == -1) {
		printf("%d ****************************** ", socketLFS);
		return 0;
	}
	inicializar_comunicacion(logger, 4); //tamanio value config
	pthread_t* hiloListening, hiloConsola, hiloEjecutor, hiloDump, hiloServidor;
	pthread_create(&hiloConsola, NULL, (void*) consola, NULL);
	//pthread_create(&hiloListening, NULL, (void*) listenSomeLQL, NULL);
	pthread_create(&hiloServidor, NULL, (void*) hilo_servidor, &socketLFS);
	pthread_create(&hiloDump, NULL, (void*) esperarTiempoDump, NULL);

	//pthread_create(&hiloEjecutor , NULL,(void*) consola, NULL);

	//pthread_join(hiloListening, NULL);
	pthread_join(hiloConsola, NULL);
	pthread_kill(hiloServidor, SIGKILL);
	pthread_join(hiloDump, NULL);
	if (socketLFS == -1) {
		close(socketLFS);
	}
	cerrarTodo();

	// consola();
	return 0;
}

/********************************************************************************************
 * 							SET UP LISANDRA, FILE SYSTEM Y COMPRIMIDOR
 ********************************************************************************************
 */

void LisandraSetUP() {

	imprimirMensajeProceso("Iniciando el modulo LISSANDRA FILE SYSTEM\n");

	logger = archivoLogCrear(LOG_PATH, "Proceso Lissandra File System");

	imprimirVerde(logger,
			"[LOG CREADO] continuamos cargando la estructura de configuracion.");

	if (cargarConfiguracion()) {
		if (!obtenerMetadata()) {
			//SI SE CARGO BIEN LA CONFIGURACION ENTONCES PROCESO DE ABRIR UN SERVIDOR
			imprimirMensajeProceso(
					"Levantando el servidor del proceso Lisandra");
			abrirServidorLissandra();
			crearBloques();
		}
	}

	log_info(logger, "la configuracion y la metadata se levantaron ok");

	memtable = dictionary_create();
	listaTablasInsertadas = list_create();
	listaRegistrosMemtable = list_create();
}

int abrirServidorLissandra() {
	return 1;
	socketEscuchaMemoria = nuevoSocket(logger);

	if (socketEscuchaMemoria == ERROR) {
		imprimirError(logger, "[ERROR] Fallo al crear Socket.");
		return -1;
	} else {
		imprimirVerde1(logger, "[  OK  ] Se ha creado el socket nro.: %d.",
				socketEscuchaMemoria);

	}

	int puerto_a_escuchar = configFile->puerto_escucha;

	imprimirMensaje1(logger, "[PUERTO] Asociando a puerto: %i.",
			puerto_a_escuchar);

	asociarSocket(socketEscuchaMemoria, puerto_a_escuchar, logger);

	imprimirMensaje(logger, "[  OK  ] Asociado.");

	socketEscuchar(socketEscuchaMemoria, 10, logger);

	return 1;

} // int abrirServidorLissandra()

bool cargarConfiguracion() {

	log_info(logger, "Por reservar memoria para variable de configuracion.");

	configFile = malloc(sizeof(t_lfilesystem_config));

	t_config* archivoCOnfig;

	log_info(logger,
			"Por crear el archivo de config para levantar archivo con datos.");

	archivoCOnfig = config_create(PATH_LFILESYSTEM_CONFIG);

	if (archivoCOnfig == NULL) {
		imprimirMensajeProceso(
				"NO se ha encontrado el archivo de configuracion\n");
		log_info(logger, "NO se ha encontrado el archivo de configuracion");
	}
	else {
		int ok = 1;
		imprimirMensajeProceso(
				"Se ha encontrado el archivo de configuracion\n");

		log_info(logger, "LissandraFS: Leyendo Archivo de Configuracion...");

		if (config_has_property(archivoCOnfig, "PUERTO_ESCUCHA")) {

			log_info(logger, "Almacenando el puerto");

			//Por lo que dice el texto
			configFile->puerto_escucha = config_get_int_value(archivoCOnfig,
					"PUERTO_ESCUCHA");

			log_info(logger, "El puerto de escucha es: %d",
					configFile->puerto_escucha);

		} else {
			imprimirError(logger,
					"El archivo de configuracion no contiene el PUERTO_ESCUCHA");
			ok--;

		}

		if (config_has_property(archivoCOnfig, "PUNTO_MONTAJE")) {

			log_info(logger, "Almacenando el PUNTO DE MONTAJE: %s",
					config_get_string_value(archivoCOnfig, "PUNTO_MONTAJE"));

			//Por lo que dice el texto		

			configFile->punto_montaje = config_get_string_value(archivoCOnfig,
					"PUNTO_MONTAJE");

			log_info(logger, "El punto de montaje es: %d",
					configFile->punto_montaje);

			tabla_Path = malloc(string_length(configFile->punto_montaje) + 8);

			tabla_Path = string_duplicate(configFile->punto_montaje);

			log_info(logger, "La variabla tabla_path queda con: %s",
					tabla_Path);

			strtok(tabla_Path, "\"");
			strtok(tabla_Path, "\"");

			string_append(&tabla_Path, "/Tables/");

			log_info(logger, "Y ahora la variabla tabla_path queda con: %s",
					tabla_Path);

		} else {
			imprimirError(logger,
					"El archivo de configuracion no contiene el PUNTO_MONTAJE");
			ok--;

		}

		if (config_has_property(archivoCOnfig, "RETARDO")) {

			log_info(logger, "Almacenando el retardo");

			//Por lo que dice el texto
			configFile->retardo = config_get_int_value(archivoCOnfig,
					"RETARDO");

			log_info(logger, "El retardo de respuesta es: %d",
					configFile->retardo);

		} else {
			imprimirError(logger,
					"El archivo de configuracion no contiene RETARDO");
			ok--;

		}

		if (config_has_property(archivoCOnfig, "TAMANIO_VALUE")) {

			log_info(logger, "Almacenando el tamanio del valor de una key");

			//Por lo que dice el texto
			configFile->tamanio_value = config_get_int_value(archivoCOnfig,
					"TAMANIO_VALUE");

			log_info(logger, "El tamanio del valor es: %d",
					configFile->tamanio_value);

		} else {
			imprimirError(logger,
					"El archivo de configuracion no contiene el TAMANIO_VALUE");
			ok--;

		}

		if (config_has_property(archivoCOnfig, "TIEMPO_DUMP")) {

			log_info(logger, "Almacenando el puerto");

			//Por lo que dice el texto
			configFile->tiempo_dump = config_get_int_value(archivoCOnfig,
					"TIEMPO_DUMP");

			log_info(logger, "El tiempo de dumpeo es: %d",
					configFile->tiempo_dump);

		} else {
			imprimirError(logger,
					"El archivo de configuracion no contiene el TIEMPO_DUMP");
			ok--;

		}

		if (ok > 0) {
			imprimirVerde(logger,
					"Se ha cargado todos los datos del archivo de configuracion");
			//	log_info(logger, "Se ha cargado todos los datos del archivo de configuracion");
			return true;

		} else {
			imprimirError(logger,
					"ERROR: No Se han cargado todos o algunos los datos del archivo de configuracion\n");
			//		imprimirMensajeProceso("ERROR: No Se han cargado todos los datos del archivo de configuracion\n");
			return false;
		}

	}

	return false;

}

int existeArchivo(char* path) {
	FILE* reader = fopen(path, "r");
	if (reader == NULL)
		return false;
	fclose(reader);
	return true;
}

void cargarBitmap() {

	bitmapPath = malloc(sizeof(char) * 50);
	bitmapPath = string_new();

	string_append(&bitmapPath, configFile->punto_montaje);

	string_append(&bitmapPath, PATH_LFILESYSTEM_BITMAP);

	if (!existeArchivo(bitmapPath)) {
		log_info(log, "Archivo de bitmap no existe, se procede a crear el bitmap");
		bitarray = crearBitarray();
	} else {
		log_info(logger, "existe archivo, se procede a abrir el bitmap");
		abrirBitmap();
	}

	log_info(logger, "cantidad de bloques libres en el bitmap: %d",
			cantBloquesLibresBitmap());

	//pruebas de las funciones bitmap
	log_info(logger, "cantidad de bloques ocupados: %d", cantidadBloquesOcupadosBitmap());

	 ocuparBloqueLibreBitmap(0);
	 log_info(logger, "ocupando bloque: %d", 0);
	 log_info(logger, "se ocupo bien? tiene que ser 1: %d", estadoBloqueBitmap(0));

	 log_info(logger, "cantidad de bloques ocupados: %d = 1?", cantidadBloquesOcupadosBitmap());
	 log_info(logger, "primer bloque libre: %d", obtenerPrimerBloqueLibreBitmap());

	 liberarBloqueBitmap(0);
	 log_info(logger, "okey... vamos a liberarlo");
	 log_info(logger, "se libero bien? tiene que ser 0: %d", estadoBloqueBitmap(0));

	 log_info(logger, "cantidad de bloques ocupados: %d = 0?", cantidadBloquesOcupadosBitmap());
}

int abrirBitmap(){

	char* fs_path = string_new();

	string_append(&fs_path,bitmapPath);

	int bitmap = open(fs_path, O_RDWR);
	struct stat mystat;

	if (fstat(bitmap, &mystat) < 0) {
		log_error(logger, "Error en el fstat\n");
		close(bitmap);
	}

	char *bmap;
	bmap = mmap(NULL, mystat.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, bitmap, 0);

	if (bmap == MAP_FAILED) {
		log_error(logger, "algo fallo en el mmap");
	}

	bitarray = bitarray_create_with_mode(bmap, metadataLFS->blocks / 8, MSB_FIRST);

	log_info(logger, "bitmap abierto correctamente");

	free(fs_path);
	return 0;
}

t_bitarray* crearBitarray() {

	bytesAEscribir = metadataLFS->blocks / 8;

	if (metadataLFS->blocks % 8 != 0)
		bytesAEscribir++;

	if (fopen(bitmapPath, "rb") != NULL) {

		return NULL;
	}

	char* bitarrayAux = malloc(bytesAEscribir);
	bzero(bitarrayAux, bytesAEscribir);

	archivoBitmap = fopen(bitmapPath, "wb");

	if (archivoBitmap == NULL) {
		imprimirError(logger,
				"El archivoBitmap no se pudo abrir correctamente");
		exit(-1);
	}

	fwrite(bitarrayAux, bytesAEscribir, 1, archivoBitmap);

	imprimirMensajeProceso(
			"[BITMAP CREADO] ya se puede operar con los bloques");

	return bitarray_create_with_mode(bitarrayAux, bytesAEscribir, MSB_FIRST);

}

void persistirCambioBitmap() {

	fwrite(bitarray->bitarray, bytesAEscribir, 1, archivoBitmap);

	log_info(logger, "cambios en bitmap persistidos");
}

int cantBloquesLibresBitmap() {
	int cantidad = 0;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {
		if (bitarray_test_bit(bitarray, i) == 0) {
			cantidad++;
		}
	}

	return cantidad;
}

int estadoBloqueBitmap(int bloque) {

	return bitarray_test_bit(bitarray, bloque);
}

int ocuparBloqueLibreBitmap(int bloque) {

	bitarray_set_bit(bitarray, bloque);
	persistirCambioBitmap();

	return 0;
}

int liberarBloqueBitmap(int bloque) {

	bitarray_clean_bit(bitarray, bloque);
	persistirCambioBitmap();

	return 0;
}

int obtenerPrimerBloqueLibreBitmap() {

	int posicion = -1;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {
		if (bitarray_test_bit(bitarray, i) == 0) {
			posicion = i;
			break;
		}
	}

	return posicion;
}

int obtenerPrimerBloqueOcupadoBitmap() {

	int posicion;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {
		if (bitarray_test_bit(bitarray, i) == 1) {
			posicion = i;
			break;
		}
	}

	return posicion;
}

int cantidadBloquesOcupadosBitmap() {

	int cantidad = 0;

	for (int i = 0; i < bitarray_get_max_bit(bitarray); i++) {
		if (bitarray_test_bit(bitarray, i) == 1) {
			cantidad++;
		}
	}

	return cantidad;
}

void consola() {

	log_info(logger, "En el hilo de consola");

	menu();

	//char bufferComando[MAXSIZE_COMANDO];-> Implementacion anterior
	char **comandoSeparado;

	while (1) {

		//printf(">");

		linea = readline(">");

		if (linea) {
			add_history(linea);

			sem_wait(&semaforoQueries);
			list_add(list_queries, linea);
			sem_post(&semaforoQueries);
		}

		if (!strncmp(linea, "exit", 4)) {
			free(linea);
			break;
		}

		//fgets(bufferComando, MAXSIZE_COMANDO, stdin); -> Implementacion anterior
		comandoSeparado = string_split(linea, separator);

		validarLinea(comandoSeparado, logger);

		free(linea);

	}
}

void menu() {

	printf(
			"Los comandos que se pueden ingresar son: \n"
					"COMANDOS [ARGUMENTOS] (* -> opcional) \n"
					"insert [TABLA] [KEY] [VALUE] [TIMESTAMP]* \n"
					"select [TABLA] [KEY]\n"
					"create [TABLA] [TIPO_CONSISTENCIA] [NRO_PARTICION] [TIEMPO_COMPACTACION]\n"
					"describe [TABLA]*\n"
					"drop [TABLA]\n"
					"SALIR \n"
					"\n");

}

void validarLinea(char** lineaIngresada, t_log* logger) {

	for (int i = 0; lineaIngresada[i] != NULL; i++) {

		log_info(logger, "En la posicion %d del array esta el valor %s", i,
				lineaIngresada[i]);

		// log_info(logger,);

		tamanio = i + 1;
	}

	log_info(logger, "El tamanio del vector de comandos es: %d", tamanio);

	switch (tamanio) {

	case 1: {
		if (strcmp(lineaIngresada[0], "describe") == 0) {

			printf("Describe seleccionado\n");

			comandoDescribe();

		} else {
			printf("Comando mal ingresado. \n");

			log_error(logger, "Opcion mal ingresada por teclado en la consola");
			break;
		}
		break;
	}
	case 2:
		validarComando(lineaIngresada, tamanio, logger);

		break;

	case 3:
		validarComando(lineaIngresada, tamanio, logger);

		break;

	case 4:
		validarComando(lineaIngresada, tamanio, logger);

		break;

	case 5:
		validarComando(lineaIngresada, tamanio, logger);

		break;

	default: {
		printf("Comando mal ingresado. \n");

		log_error(logger, "Opcion mal ingresada por teclado en la consola");
	}

		break;

	}
}

void validarComando(char** comando, int tamanio, t_log* logger) {

	int resultadoComando = buscarComando(comando[0], logger);

	int tamanioCadena = 0;

	switch (resultadoComando) {

	case Select: {
		printf("Se selecciono Select\n");

		log_info(logger, "Se selecciono select");

		if (tamanio == 3) {

			log_info(logger,
					"Cantidad de parametros correctos ingresados para el comando select");

			int resultado = comandoSelect(comando[1], comando[2]);

			log_info(logger, "El resultado de la operacion fue: %d", resultado);

		}

	}
		break;

	case insert: {
		printf("Insert seleccionado\n");

		log_info(logger, "Se selecciono insert");

		if (tamanio == 4 || tamanio == 5) {

			if (tamanio == 4) {
				comandoInsertSinTimestamp(comando[1], comando[2], comando[3]);
			} else {
				comandoInsert(comando[1], comando[2], comando[3], comando[4]);
			}

		}

	}
		break;

	case create: {
		printf("Create seleccionado\n");
		log_info(logger, "Se selecciono Create");

		if (tamanio == 5) {

			log_info(logger,
					"Cantidad de parametros correctos ingresados para el comando Create");

			comandoCreate(comando[1], comando[2], comando[3], comando[4]);

		}

	}
		break;

	case describe: {
		printf("Describe seleccionado\n");
		log_info(logger, "Se selecciono Describe");

		if (tamanio == 2) {

			log_info(logger,
					"Cantidad de parametros correctos ingresados para el comando Describe");

			mensaje = malloc(string_length(comando[1]));

			strcpy(mensaje, comando[1]);

			comandoDescribeEspecifico(comando[1]);

			//log_info(logger, "En mensaje ya tengo: %s y es de tamanio: %d",mensaje, string_length(mensaje));

			//log_info(logger, "Por llamar a enviarMensaje");

		}

	}
		break;

	case drop: {
		printf("Drop seleccionado\n");
		log_info(logger, "Se selecciono Drop");

		if (tamanio == 2) {

			log_info(logger,
					"Cantidad de parametros correctos ingresados para el comando Drop");

			mensaje = malloc(string_length(comando[1]) + 1);

			strcpy(mensaje, comando[1]);

			log_info(logger, "Queriamos mandar esto: %s", comando[1]);
			log_info(logger, "Y se mando esto: %s", mensaje);

			comandoDrop(comando[1]);

		}

	}
		break;

	case salir: {
		printf("Salir seleccionado\n");
		log_info(logger, "Se selecciono Salir");

		//cerrarTodo(); terminar

	}
		break;

	default: {
		printf("Comando mal ingresado. \n");
		log_error(logger, "Opcion mal ingresada por teclado en la consola");
	}
		break;

	}
}

int buscarComando(char* comando, t_log* logger) {

	log_info(logger, "Recibimos el comando: %s", comando);

	int i = 0;

	for (i; i <= salir && strcmp(comandosPermitidos[i], comando); i++) {
	}

	log_info(logger, "Se devuelve el valor %d", i);

	return i;

}

// LOGGEA todo lo que escucha.
void listenSomeLQL() {

	//char bufferComando[MAXSIZE_COMANDO];-> Implementacion anterior
	char **comandoSeparado;

	while (1) {

		imprimirMensaje(logger,
				" \n ====== LFS Listener: waiting for client connections ====== \n ");

		conexionEntrante = aceptarConexionSocket(socketEscuchaMemoria, logger);

		//Puntero buffer = (void*)string_new(); // malloc(sizeof(char)*100);

		Puntero buffer = malloc(sizeof(char) * 100);

		recibiendoMensaje = socketRecibir(conexionEntrante, buffer, 50, logger);

		char* msg = string_new();

		// char * msg = malloc(sizeof(char)*100);
		msg = string_duplicate(buffer); // <-- Esto hace funcionar del string por red.

		sem_wait(&semaforoQueries);
		list_add(list_queries, msg);
		sem_post(&semaforoQueries);

		comandoSeparado = string_split(msg, separator);

		validarLinea(comandoSeparado, logger);

		string_append(&msg, "Mensaje recibido: \"");
		string_append(&msg, buffer);
		string_append(&msg, "\".");

		imprimirVerde(logger, msg);
		// liberar msg?
		free(buffer);

	}

}

int verificarTabla(char* tabla) {

	tablaAverificar = malloc(string_length(tabla_Path) + string_length(tabla));

	log_info(logger,
			"Se reservo memoria para contatenar punto de montaje con la tabla");
	tablaAverificar = string_new();

	for (int i = 0; i < strlen(tabla); i++) {
		tabla[i] = toupper(tabla[i]);
	}

	string_append(&tablaAverificar, tabla_Path);
	string_append(&tablaAverificar, tabla);
	log_info(logger, "Concatenamos: %s a tablaAVerificar", tabla);
	log_info(logger,
			"[VERIFICADO] La direccion de la tabla que se quiere verificar es: %s",
			tablaAverificar);

	path_tabla_metadata = string_duplicate(tablaAverificar);
	string_append(&path_tabla_metadata, "/metadata");

	FILE *file;

	file = fopen(tablaAverificar, "r");

	if (file == NULL) {
		log_error(logger, "[ERROR] No existe la tabla");
		return -1;

	} else {
		log_info(logger, "[ OK ] La tabla ya existe.");
		fclose(file);
		return 0;
	}

}

int obtenerMetadataTabla(char* tabla) {

	log_info(logger, "[obtenerMetadata] (+) metadata a abrir : %s",
			path_tabla_metadata);

	int result = 0;

	metadata = malloc(sizeof(t_metadata_tabla)); // Vatiable global.

	t_config* metadataFile;
	metadataFile = config_create(path_tabla_metadata);

	if (metadataFile != NULL) {

		log_info(logger, "LFS: Leyendo metadata...");

		if (config_has_property(metadataFile, "CONSISTENCY")) {

			log_info(logger, "Almacenando consistencia");
			// PROBLEMA.
			metadata->consistency = config_get_string_value(metadataFile,
					"CONSISTENCY");

			log_info(logger, "La consistencia  es: %s", metadata->consistency);

		} else {

			log_error(logger, "El metadata no contiene la consistencia");

		} // if (config_has_property(metadataFile, "CONSISTENCY"))

		if (config_has_property(metadataFile, "PARTITIONS")) {

			log_info(logger, "Almacenando particiones");

			metadata->particiones = config_get_int_value(metadataFile,
					"PARTITIONS");

			log_info(logger, "Las particiones son : %d", metadata->particiones);

		} else {

			log_error(logger, "El metadata no contiene particiones");

		} // if (config_has_property(metadataFile, "PARTITIONS"))

		if (config_has_property(metadataFile, "COMPACTION_TIME")) {

			metadata->compaction_time = config_get_int_value(metadataFile,
					"COMPACTION_TIME");

			log_info(logger, "el tiempo de compactacion es: %d",
					metadata->compaction_time);

		} else {

			log_error(logger,
					"El metadata no contiene el tiempo de compactacion");

		} // if (config_has_property(metadataFile, "COMPACTION_TIME"))

	} else {

		log_error(logger,
				"[ERROR] Archivo metadata de particion no encontrado.");

		result = -1;

	} // if (metadataFile != NULL)

	log_info(logger,
			"[FREE] variable metadataFile utlizada para navegar el metadata.");

	free(metadataFile);

	//log_info(logger, "[obtenerMetadata] (-) metadata a abrir : %s",tablaAverificar);

	return result;

}

int obtenerMetadata() {

	log_info(logger, "levantando metadata del File System");

	int result = 0;

	metadataLFS = malloc(sizeof(t_metadata_LFS)); // Vatiable global.

	char* metadataPath = malloc(sizeof(char) * 50);
	metadataPath = string_new();

	string_append(&metadataPath, configFile->punto_montaje);

	string_append(&metadataPath, PATH_LFILESYSTEM_METADATA);

	log_info(logger, "ruta de la metadata: %s", metadataPath);

	t_config* metadataFile;
	metadataFile = config_create(metadataPath);

	if (metadataFile != NULL) {

		log_info(logger, "LFS: Leyendo metadata...");

		if (config_has_property(metadataFile, "BLOCK_SIZE")) {

			log_info(logger, "Almacenando tamanio de bloque");
			// PROBLEMA.
			metadataLFS->block_size = config_get_int_value(metadataFile,
					"BLOCK_SIZE");

			log_info(logger, "el tamanio del bloque es: %d",
					metadataLFS->block_size);

		} else {

			log_error(logger,
					"El metadata no contiene el tama��ano de bloque [BLOCK_SIZE]");

		} // if (config_has_property(metadataFile, "CONSISTENCY"))

		if (config_has_property(metadataFile, "BLOCKS")) {

			log_info(logger, "Almacenando cantidad de bloques");

			metadataLFS->blocks = config_get_int_value(metadataFile, "BLOCKS");

			log_info(logger, "La cantidad de bloques es: %d",
					metadataLFS->blocks);

		} else {

			log_error(logger,
					"El metadata no contiene cantidad de bloques [BLOCKS]");

		} // if (config_has_property(metadataFile, "PARTITIONS"))

		if (config_has_property(metadataFile, "MAGIC_NUMBER")) {

			log_info(logger, "Almacenando magic number");

			metadataLFS->magic_number = config_get_string_value(metadataFile,
					"MAGIC_NUMBER");

			log_info(logger, "el magic number es: %s",
					metadataLFS->magic_number);

		} else {

			log_error(logger,
					"El metadata no contiene el magic number [MAGIC_NUMBER]");

		} // if (config_has_property(metadataFile, "COMPACTION_TIME"))

	} else {

		log_error(logger,
				"[ERROR] Archivo metadata de file system no encontrado.");

		result = -1;

	} // if (metadataFile != NULL)

	log_info(logger,
			"[FREE] variable metadataFile utlizada para navegar el metadata.");

	free(metadataFile);

	log_info(logger, "result: %d", result);

	return result;
}

char* retornarValores(char* tabla) {

	log_info(logger, "llego aca");

	printf("\n");
	printf("Valores de la %s \n", tabla);
	printf("\n");
	printf("CONSISTENCY=%s \n", metadata->consistency);
	printf("PARTITIONS=%d \n", metadata->particiones);
	printf("COMPACTION_TIME=%d \n", metadata->compaction_time);

	char* particiones = malloc(4);
	char* tiempoCompactacion = malloc(7);

	sprintf(particiones, "%d", metadata->particiones);
	sprintf(tiempoCompactacion, "%d", metadata->compaction_time);

	char* valorDescribe = malloc(
			strlen(tabla) + strlen(metadata->consistency) + strlen(particiones)
					+ strlen(tiempoCompactacion) + 4);
	valorDescribe = string_new();

	string_append(&valorDescribe, tabla);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, metadata->consistency);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, particiones);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, tiempoCompactacion);

	return valorDescribe;

}

char* retornarValoresDirectorio() {
	DIR *dir;
	struct dirent *ent;

	char* pathTabla = malloc(sizeof(char) * 50);
	char* resultado;
	int memoriaParaMalloc = 0;
	pathTabla = string_new();
	t_list* lista_describes;
	lista_describes = list_create();

	string_append(&pathTabla, configFile->punto_montaje);
	string_append(&pathTabla, TABLE_PATH);

	dir = opendir(pathTabla);

	//log_info(logger,"directorio %s",dir);

	if (dir == NULL) {
		log_error(logger, "No puedo abrir el directorio");
		perror("No puedo abrir el directorio");

	}

	while ((ent = readdir(dir)) != NULL) {

		if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0) {
			log_info(logger, "Tabla analizada= %s", ent->d_name);
			verificarTabla(ent->d_name);
			obtenerMetadataTabla(ent->d_name);
			resultado = retornarValores(ent->d_name);
			log_info(logger, "el resultado es %s", resultado);
			memoriaParaMalloc += strlen(resultado) + 1; // el 1 es por el | para separar cada describe
			log_info(logger, "tamanio malloc es %d", memoriaParaMalloc);
			list_add(lista_describes, resultado);

		}
	}
	char* resultadoFinal = malloc(memoriaParaMalloc + 1);
	resultadoFinal = string_new();
	for (int i = 0; i < list_size(lista_describes); i++) {
		char* elemento = list_get(lista_describes, i);
		string_append(&resultadoFinal, elemento);
		string_append(&resultadoFinal, "|");
	}
	resultadoFinal = stringTomarDesdeInicio(resultadoFinal,
			strlen(resultadoFinal) - 1);
	return resultadoFinal;
	closedir(dir);
}

void INThandler(int sig) {

     char  c;

     signal(sig, SIG_IGN);
     printf("OUCH, did you hit Ctrl-C?\n");
            //"Do you really want to quit? [y/n] ");
     /*c = getchar();
     if (c == 'y' || c == 'Y')
          exit(0);
     else
          signal(SIGINT, INThandler);
     getchar(); // Get new line character*/
}

//DUMP

void esperarTiempoDump() {

	while (true) {

		//usleep(configFile->tiempo_dump*1000);
		sleep(5);
		log_info(logger, "Es tiempo de dump, hay cosas en la memtable?");
		mutexBloquear(&listaTablasInsertadas_mx);
		int tam = list_size(listaTablasInsertadas);
		mutexDesbloquear(&listaTablasInsertadas_mx);
		if (tam > 0) {
			signal(SIGINT, INThandler);
			log_info(logger, "Se encontraron cosas, se hace el dump");
			sleep(20);
			realizarDump();
			cantidad_de_dumps++;
			pause();
		} else {
			log_info(logger, "La memtable esta vacia");
		}

	}

}

void realizarDump() {
	mutexBloquear(&listaTablasInsertadas_mx);
	int tam = list_size(listaTablasInsertadas);
	mutexBloquear(&memtable_mx);
	for (int i = 0; i < tam; i++) {
		char* tabla = list_get(listaTablasInsertadas, i);
		indiceTablaParaTamanio = i;
		log_info(logger, "la tabla insertada en la memtable es %s", tabla);
		char* path = armarPathTablaParaDump(tabla, cantidad_de_dumps);
		crearArchivoTemporal(path, tabla);
		//tamanioRegistros[i] = 0;
	}
	log_info(logger, "Se limpia diccionario y la listaTablasInsertadas");
	dictionary_clean(memtable);
	mutexDesbloquear(&memtable_mx);
	list_clean(listaTablasInsertadas);
	mutexDesbloquear(&listaTablasInsertadas_mx);
}

char* armarPathTablaParaDump(char* tabla, int dumps) {
	char *nombreArchivoTemporal = malloc(sizeof(char) * 3);
	char* path_archivo_temporal = malloc(
			string_length(TABLE_PATH) + string_length(configFile->punto_montaje)
					+ string_length(tabla)
					+ string_length(nombreArchivoTemporal)
					+ string_length(PATH_TMP) + 2);

	path_archivo_temporal = string_new();

	string_append(&path_archivo_temporal, configFile->punto_montaje);

	string_append(&path_archivo_temporal, TABLE_PATH);

	string_append(&path_archivo_temporal, tabla);

	string_append(&path_archivo_temporal, "/");

	sprintf(nombreArchivoTemporal, "%d", dumps);

	string_append(&path_archivo_temporal, nombreArchivoTemporal);

	string_append(&path_archivo_temporal, PATH_TMP);

	log_info(logger, "la ruta es %s", path_archivo_temporal);

	return path_archivo_temporal;

}

int crearArchivoTemporal(char* path, char* tabla) {

	// path objetivo: /home/utnso/tp-2019-1c-mi_ultimo_segundo_tp/LissandraFileSystem/Tables/TABLA/cantidad_de_dumps.tmp

	t_list* listaRegistrosTabla = dictionary_get(memtable, tabla);
	t_list* bloquesUsados = list_create();
	int tam_total_registros = tamTotalListaRegistros(listaRegistrosTabla);
	int cantidad_bloques = cuantosBloquesNecesito(tam_total_registros);
	int bloqueAux;

	if(cantBloquesLibresBitmap() >= cantidad_bloques) {
		for(int i=0; i<cantidad_bloques; i++) {
			bloqueAux = obtenerPrimerBloqueLibreBitmap();
			if(bloqueAux != -1) {
				ocuparBloqueLibreBitmap(bloqueAux);
				//list_add(bloquesUsados, (void*)bloqueAux);
				list_add(bloquesUsados, &bloqueAux);
			}
			else {
				//liberar los bloques de la lista
				return -1;
			}
		}
	}
	else {
		log_error(logger, "[DUMP] no hay bloques disponibles para hacer el dump");
	}

	void* bufferRegistros = malloc(tam_total_registros);
	bufferRegistros = armarBufferConRegistros(listaRegistrosTabla, tam_total_registros);
	int resultadoEscritura = escribirVariosBloques(bloquesUsados, tam_total_registros, bufferRegistros);

	if(resultadoEscritura != -1){
		FILE* temporal;
		temporal = fopen(path, "w");
		log_info(logger, "[DUMP] creamos el archivo, ahora  lo llenamos");
		log_info(logger, "[DUMP] path del archivo %s",path);

		if(temporal != NULL) {
			char* contenido = malloc(
					string_length("SIZE=") + sizeof(char)*2
							+ string_length("BLOCK=[]") + sizeof(char)*2*list_size(bloquesUsados) - 1); //arreglar para bloques con mas de un numero (string, no char)
			contenido = string_new();
			string_append(&contenido, "SIZE=");
			char* size = malloc(10);
			sprintf(size, "%d", tam_total_registros);
			string_append(&contenido,size);
			string_append(&contenido, "\n");
			string_append(&contenido, "BLOCK=[");
			char* bloque = malloc(10);
			for(int i=0; i<list_size(bloquesUsados); i++){
				int* nroBloque = list_get(bloquesUsados, i);
				sprintf(bloque, "%d", *nroBloque);
				string_append(&contenido, bloque);
				string_append(&contenido, ",");
			}
			contenido = stringTomarDesdeInicio(contenido, strlen(contenido) - 1);
			string_append(&contenido, "]");
			fputs(contenido, temporal);
			log_info(logger, "[DUMP] Temporal completado con: %s", contenido);
			fclose(temporal);
		}
		else {
			//liberar bloques de la lista de bloquesUsados
			log_error(logger, "[DUMP] error al abrir el archivo temporal con path: %s", path);
		}
	}
	else {
		log_error(logger, "[DUMP] hubo un error al escribir los datos en los bloques");
	}


	return 0;
}

//OPERACIONES CON BLOQUES

int tamTotalListaRegistros(t_list* listaRegistros) {
	int cantidad_registros = list_size(listaRegistros);
	log_info(logger, "cantidad de registros de la lista: %d", cantidad_registros);

	int tam_total_registros = 0;
	t_registroMemtable* registro;

	for (int i = 0; i < cantidad_registros; i++) {
		registro = list_get(listaRegistros, i);

		tam_total_registros += registro->tam_registro - 1; // El -1 porque no estoy escribiendo el \0 al archivo, si no al leer le sobran bytes
	}

	tam_total_registros += sizeof(char) * 3 * cantidad_registros;

	log_info(logger, "tamanio total de registros: %d", tam_total_registros);

	return tam_total_registros;
}

int cuantosBloquesNecesito(int tam_total) {
	if(tam_total % metadataLFS->block_size == 0) {
		return tam_total / metadataLFS->block_size;
	}

	return tam_total / metadataLFS->block_size + 1;
}

void* armarBufferConRegistros(t_list* listaRegistros, int tam_total_registros) {

	int offset = 0;
	char punto_y_coma = ';';
	char barra_n = '\n';

	int cantidad_registros = list_size(listaRegistros);
	t_registroMemtable* registro;

	void* bufferConRegistros = malloc(tam_total_registros);

	for (int i = 0; i < cantidad_registros; i++) {

		registro = list_get(listaRegistros, i);
		memcpy(bufferConRegistros + offset, &registro->timestamp,
				sizeof(double));
		offset += sizeof(double);
		memcpy(bufferConRegistros + offset, &punto_y_coma, sizeof(char));
		offset += sizeof(char);
		memcpy(bufferConRegistros + offset, &registro->key,
				sizeof(u_int16_t));
		offset += sizeof(u_int16_t);
		memcpy(bufferConRegistros + offset, &punto_y_coma, sizeof(char));
		offset += sizeof(char);
		memcpy(bufferConRegistros + offset, registro->value,
				strlen(registro->value));
		offset += strlen(registro->value);
		memcpy(bufferConRegistros + offset, &barra_n, sizeof(char));
		offset += sizeof(char);
	}

	return bufferConRegistros;
}

int escribirVariosBloques(t_list* bloques, int tam_total_registros, void* buffer) {

	int resultado = 1;
	int offset = 0;

	for(int i=0; i<list_size(bloques); i++) {

		log_info(logger,"entro al for");
		int* auxnroBloque = list_get(bloques, i);
		int nroBloque = *auxnroBloque;
		log_info(logger,"numero de bloque %d",nroBloque);
		if(tam_total_registros <= metadataLFS->block_size) {
			log_info(logger,"tam registros menor a block size");
			resultado = escribirBloque(nroBloque, tam_total_registros, offset, buffer);
			offset += tam_total_registros;
			tam_total_registros -= tam_total_registros;
		}
		else {
			log_info(logger,"tam registros mayor a block size");
			resultado = escribirBloque(nroBloque, metadataLFS->block_size, offset, buffer);
			tam_total_registros -= metadataLFS->block_size;
			offset += metadataLFS->block_size;
		}

		if(resultado == -1) {
			log_info(logger,"resultado -1");
			//liberar bloques en el bitmap
			break;
		}
		log_info(logger,"salgo del for");
	}

	log_info(logger, "[BLOQUE] Se terminaron de escribir los bloques");

	return resultado;
}

int escribirBloque(int bloque, int size, int offset, void* buffer) {


    char* path = crearPathBloque(bloque);
    log_info(logger,"path de bloque a escribir %s",path);
    FILE* bloqueFile = fopen(path,"wb");

    if(bloqueFile != NULL){

    	log_info(logger,"entre al if");
		fwrite(buffer+offset, size, 1, bloqueFile);
		fclose(bloqueFile);
		leerBloque(path);
		free(path);
		return 0;
    }

    log_info(logger, "[BLOQUE] bloque %d escrito con exito", bloque);
    free(path);
    return -1;
}

void crearBloques(){

	int tamanio = strlen(configFile->punto_montaje)+strlen(PATH_BLOQUES)+30;
	char* pathBloque = malloc(tamanio);
	log_info(logger,"Voy a crear los bloques");
	for (int i = 0; i < metadataLFS->blocks; i++) {
					snprintf(pathBloque,tamanio,"%s%sblock%d.bin",configFile->punto_montaje,PATH_BLOQUES,i);
					FILE* bloque;
					bloque = fopen(pathBloque, "a");
					fclose(bloque);


}
	log_info(logger,"los bloques fueron creados");
	free(pathBloque);
}

char* crearPathBloque(int bloque){

		int tamanio = strlen(configFile->punto_montaje)+strlen(PATH_BLOQUES)+30;
		char* pathBloque = malloc(tamanio);
	   snprintf(pathBloque,tamanio,"%s%sblock%d.bin",configFile->punto_montaje,PATH_BLOQUES,bloque);

return pathBloque;
}

t_list* leerBloque(char* path) {
	t_list *registros_leidos = list_create();
	FILE* bloque;
	int tam_bloque;
	bloque = fopen(path, "rb");
	t_registroMemtable* registro;

	fseek(bloque, 0, SEEK_END);
	tam_bloque = ftell(bloque);

	log_info(logger, "[BLOQUE] tam del bloque: %d", tam_bloque);

	rewind(bloque);

	void* registros_bloque = malloc(tam_bloque);

	if (fread(registros_bloque, tam_bloque, 1, bloque)) {

		int offset = 0;
		char *aux = malloc(configFile->tamanio_value + 1);
		while (offset < tam_bloque) {
			registro = malloc(sizeof(t_registroMemtable));

			//Guardo timestamp
			memcpy(&registro->timestamp, registros_bloque + offset,
					sizeof(double));
			offset += sizeof(double);
			offset += sizeof(char); // ";"

			//Guardo key
			memcpy(&registro->key, registros_bloque + offset, sizeof(uint16_t));
			offset += sizeof(uint16_t);
			offset += sizeof(char); // ";"

			//Guardo en el aux el máximo tamaño del value
			if (configFile->tamanio_value + 1 <= tam_bloque - offset)
				memcpy(aux, registros_bloque + offset,
						configFile->tamanio_value + 1);
			else
				memcpy(aux, registros_bloque + offset, tam_bloque - offset);

			//Busco el \n que indica el fin del valor
			char **aux_split = string_split(aux, "\n");
			registro->value = malloc(strlen(aux_split[0]) + 1);
			strcpy(registro->value, aux_split[0]);

			//Libero toda la memoria que genera el string_split
			int i = 0;
			while (aux_split[i] != NULL) {
				free(aux_split[i]);
				i++;
			}
			free(aux_split);

			//Avanzo el offset solo el tamaño realmente leído del valor
			offset += strlen(registro->value) + sizeof(char); //value + \n

			//Calculo tamaño
			registro->tam_registro = strlen(registro->value) + 1
					+ sizeof(double) + sizeof(uint16_t);

			log_info(logger, "timestamp leido: %lf", registro->timestamp);
			log_info(logger, "key leida: %d", registro->key);
			log_info(logger, "value leido: %s", registro->value);
			log_info(logger, "tamaño registro: %d", registro->tam_registro);

			//Agrego el registro a la lista que voy a retornar
			list_add(registros_leidos, registro);
			log_info(logger, "Agregado a la lista");
			log_info(logger, "Hasta ahora lei %d bytes de %d", offset,
					tam_bloque);
		}
		free(aux);
		fclose(bloque);
	}
	else {
		log_error(logger, "[BLOQUE] no se pudo leer el archivo con el path: %s", path);
	}

	return registros_leidos;
}

//OPERACIONES CON BLOQUES


//DUMP
int determinarParticion(int key, int particiones) {

	log_info(logger, "KEY: %d ", key);

	int retornar = key % particiones;

	log_info(logger, "PARTICION: %d ", retornar);

	return retornar;

}

void rutaParticion(int particion) {
	char * stringParticion = malloc(sizeof(char) * 3);

	sprintf(stringParticion, "%d", particion);

	archivoParticion = malloc(
			string_length(tablaAverificar) + string_length(stringParticion)
					+ string_length(PATH_BIN));

	log_info(logger,
			"Se reservo memoria para concatenar ruta de la tabla con la particion");
	archivoParticion = string_new();
	string_append(&archivoParticion, tablaAverificar);

	string_append(&archivoParticion, "/");

	string_append(&archivoParticion, stringParticion);

	string_append(&archivoParticion, PATH_BIN); // ".bin"

	log_info(logger, "%s", archivoParticion);
}

void escanearParticion(int particion) {

	log_info(logger, "[escanearParticion] (+) ");

	log_info(logger, "[escanearParticion] (+) %s", tabla_Path);

	log_info(logger, "[escanearParticion] (+) %s", tablaAverificar);

	rutaParticion(particion);
	log_info(logger, "[escanearParticion] (+) Sobrevivi nro 1");
	particionTabla = malloc(sizeof(t_particion));

	t_config* particionFile;

	particionFile = config_create(archivoParticion);
	log_info(logger, "[escanearParticion] (+) Sobrevivi nro 2");

	FILE *file;
	file = fopen(archivoParticion, "r");
	if (file == NULL) {
		log_error(logger, "No existe la particion");
		perror("Error");
	} else {
		log_info(logger, "Abrimos particion %d", particion);
		fclose(file);
	}

	if (particion > -1) {

		log_info(logger, "LFS: Leyendo metadata de la particion...");

		if (config_has_property(particionFile, "SIZE")) {

			log_info(logger, "Almacenando el tamanio de la particion");

			particionTabla->size = config_get_int_value(particionFile, "SIZE");

			log_info(logger, "el tamanio de la particion  es: %d",
					particionTabla->size);
		} else {
			log_error(logger, "El metadata de la tabla no contiene el tamanio");

		}
		if (config_has_property(particionFile, "BLOCKS")) {

			log_info(logger, "Almacenando los bloques");

			particionTabla->bloques = config_get_array_value(particionFile,
					"BLOCKS");

			log_info(logger, "Las bloques son : %s",
					particionTabla->bloques[0]);
		} else {
			log_error(logger, "El metadata de la tabla no contiene bloques");

		}

	} else {

		log_error(logger,
				"No se encontro el metadata para cargar la estructura");

	}

	log_info(logger,
			"Cargamos todo lo que se encontro en el metadata. Liberamos la variable metadataFile que fue utlizada para navegar el metadata");

	free(particionFile);

	log_info(logger, "[escanearParticion] (-) ");

}

/*
char* buscarBloque(char* key) {


	char* bloqueObjetivo = malloc(
			string_length(configFile->punto_montaje)
					+ string_length(PATH_BLOQUES) + 11);

	log_info(logger, "Se reservo memoria para concatenar ruta de blqoues");

	bloqueObjetivo = string_new();

	string_append(&bloqueObjetivo, configFile->punto_montaje);

	log_info(logger, "BloqueObjetivo: %s", bloqueObjetivo); // 1er linea de direccion

	string_append(&bloqueObjetivo, PATH_BLOQUES);

	log_info(logger, "BloqueObjetivo: %s", bloqueObjetivo); // 2da linea de direccion

	char* bloque = malloc(sizeof(particionTabla->bloques)); // 3er linea de direccion
	bloque = particionTabla->bloques[0];

	string_append(&bloqueObjetivo, "block");
	string_append(&bloqueObjetivo, bloque);
	string_append(&bloqueObjetivo, ".bin");
	log_info(logger, "BloqueObjetivo: %s", bloqueObjetivo);

	/*FILE *file;
	file = fopen(bloqueObjetivo, "r");

	if (file == NULL) {
		//log_error(logger, "No existe la particion");
		perror("Error");
	} else {

		log_info(logger, "Abrimos Bloque");
		//char lectura;
		//do {
		//	do {
		//		lectura = fgetc(file);
		//		printf("%c", lectura);
		//	} while (lectura != '\n');
		//	printf("fin de linea \n");
		//	lectura = fgetc(file);
		//} while (!feof(file));

		char linea[1024];

		while (fgets(linea, 1024, (FILE*) file)) {
			printf("LINEA: %s", linea);
		}

		fclose(file);

	}

	return bloqueObjetivo;


}
*/
void eliminarTablaCompleta(char* tabla) {

	if (obtenerMetadataTabla(tabla) == 0) {

		for (int i = 0; i < metadata->particiones; i++) {

			rutaParticion(i);

			log_info(logger,
					"Vamos a eliminar el archivo binario  de la tabla: %s",
					archivoParticion);

			int retParticion = remove(archivoParticion);

			if (retParticion == 0) { // Eliminamos el archivo
				log_info(logger,
						"El archivo fue eliminado satisfactoriamente\n");
			} else {
				log_info(logger, "No se pudo eliminar el archivo\n");

			}
		}

	}

	for (int j = 0; j <= cantidad_de_dumps; j++) {

		char* archivoTemporal = armarPathTablaParaDump(tabla, j);
		char* archivoTemporalC = armarPathTablaParaDump(tabla, j);
		string_append(&archivoTemporalC, "c");

		log_info(logger,
				"Vamos a eliminar el archivo temporal  de la tabla: %s",
				archivoTemporal);

		log_info(logger,
				"Vamos a eliminar el archivo temporal a compactar de la tabla: %s",
				archivoTemporalC);

		int retTemporal = remove(archivoTemporal);
		int retTemporalC = remove(archivoTemporalC);

		if (retTemporal == 0) { // Eliminamos el archivo
			log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
		} else {
			log_info(logger, "No se pudo eliminar el archivo\n");
		}

		if (retTemporalC == 0) { // Eliminamos el archivo
			log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
		} else {
			log_info(logger, "No se pudo eliminar el archivo\n");
		}

	}

	log_info(logger, "Vamos a eliminar el metadata de la tabla: %s",
			path_tabla_metadata);

	int retMet = remove(path_tabla_metadata);

	log_info(logger, "Resultado de remove del metadata de la tabla%d", retMet);

	if (retMet == 0) { // Eliminamos el archivo
		log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
	} else {
		log_info(logger, "No se pudo eliminar el archivo\n");
	}

	log_info(logger, "Vamos a eliminar el directorio: %s", tablaAverificar);

	int retTab = remove(tablaAverificar);

	log_info(logger, "Resultado de remove de la tabla %d", retTab);

	if (retTab == 0) { // Eliminamos el archivo
		log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
	} else {
		log_info(logger, "No se pudo eliminar el archivo\n");
	}

}

bool validarValue(char* value) {

	bool contienePuntoYcoma = stringContiene(value, ";");

	if (contienePuntoYcoma) {

		log_info(logger, "el value contiene ; por lo tanto no se agrega");

	}

	return contienePuntoYcoma;

}

bool validarKey(char* key) {

	bool contienePuntoYcoma = stringContiene(key, ";");

	if (contienePuntoYcoma) {

		log_info(logger, "la key contiene ; por lo tanto no se agrega");

	}

	return contienePuntoYcoma;

}

char* desenmascararValue(char* value) {

	char* valueSinPimeraComilla = stringTomarDesdePosicion(value, 1);
	char* valueDesenmascarado = strtok(valueSinPimeraComilla, "\"");
	log_info(logger, "el value desenmascarado es %s", valueDesenmascarado);
	return valueDesenmascarado;

}

t_registroMemtable* armarEstructura(char* value, char* key, char* timestamp) {

	t_registroMemtable* registroMemtable;
	registroMemtable = malloc(sizeof(t_registroMemtable));

	int tam_registro = strlen(value) + 1 + sizeof(u_int16_t) + sizeof(double); //es un long no un double
	registroMemtable->tam_registro = tam_registro;
	registroMemtable->value = value;
	double timestampRegistro = atof(timestamp);
	registroMemtable->timestamp = timestampRegistro;
	u_int16_t keyRegistro = strtol(key, NULL, 16);
	registroMemtable->key = keyRegistro;

	//log_info(logger,"El registro quedo conformado por: \n");
	//log_info(logger,"Value = %s ",registroMemtable->value);
	//log_info(logger,"Timestamp = %f ",registroMemtable->timestamp);
	//log_info(logger,"Key = %x ",registroMemtable->key);
	//log_info(logger,"Se procede a agregar el registro a la memtable");

	return registroMemtable;

}

int comandoSelect(char* tabla, char* key) {

	if (verificarTabla(tabla) == -1) {
		return -1;
	} else { // archivo de tabla encontrado
		int returnObtenerMetadata = obtenerMetadataTabla(tabla);
		if (returnObtenerMetadata == -1) {
			return -2;
		}; // 0: OK. -1: ERROR. // frenar en caso de error

		int valorKey = atoi(key);
		int particiones = determinarParticion(valorKey, metadata->particiones);

		escanearParticion(particiones);

		//char* keyEncontrado = buscarBloque(key); // GUardar memoria

		// ver key con timestamp mas grande

		return valorKey;

	}
} // int comandoSelect(char* tabla, char* key)

int comandoDrop(char* tabla) {

	log_info(logger, "Por verificar tabla");

	int retornoVerificar = verificarTabla(tabla);
	if (retornoVerificar == 0) {

		log_info(logger, "Vamos a eliminar la tabla: %s", tablaAverificar);

		eliminarTablaCompleta(tabla);
		return retornoVerificar;
	} else {
		return -1;
	}

}

int comandoCreate(char* tabla, char* consistencia, char* particiones,
		char* tiempoCompactacion) {

	int retornoVerificar = verificarTabla(tabla);
	if (retornoVerificar == -1) {     // La tabla no existe, se crea

		mkdir(tablaAverificar, 0777);

		log_info(logger, "Se crea la tabla y su direccion es %s ",
				tablaAverificar);
		log_info(logger, "Por crear archivo metadata");

		FILE* archivoMetadata;

		archivoMetadata = fopen(path_tabla_metadata, "w");

		if (archivoMetadata != NULL) {
			log_info(logger,
					"El archivo metadata se creo satisfactoriamente\n");

			char *lineaConsistencia = malloc(
					sizeof(consistencia) + sizeof("CONSISTENCY=") + 1);
			lineaConsistencia = string_new();
			string_append(&lineaConsistencia, "CONSISTENCY=");
			string_append(&lineaConsistencia, consistencia);
			string_append(&lineaConsistencia, "\n");
			log_info(logger, "Se agrego la consistencia %s", lineaConsistencia);

			char *lineaParticiones = malloc(
					sizeof(particiones) + sizeof("PARTITIONS=") + 1);
			lineaParticiones = string_new();
			string_append(&lineaParticiones, "PARTITIONS=");
			string_append(&lineaParticiones, particiones);
			string_append(&lineaParticiones, "\n");
			log_info(logger, "Se agregaron las particiones %s",
					lineaParticiones);

			char *lineaTiempoCompactacion = malloc(
					sizeof(tiempoCompactacion) + sizeof("COMPACTION_TIME="));
			lineaTiempoCompactacion = string_new();
			string_append(&lineaTiempoCompactacion, "COMPACTION_TIME=");
			string_append(&lineaTiempoCompactacion, tiempoCompactacion);
			log_info(logger, "Se agrego el tiempo de compactacion %s",
					lineaTiempoCompactacion);

			fputs(lineaConsistencia, archivoMetadata);
			fputs(lineaParticiones, archivoMetadata);
			fputs(lineaTiempoCompactacion, archivoMetadata);

			fclose(archivoMetadata);

			log_info(logger, "Por crear particiones");

			int aux = atoi(particiones);

			log_info(logger, "aux=%d", aux);
			for (int i = 0; i < aux; i++) {
				rutaParticion(i);
				FILE* particion;
				particion = fopen(archivoParticion, "w");
				char* lineaParticion = malloc(
						string_length("SIZE=") + sizeof(int)
								+ string_length("BLOCK=[]") + sizeof(int) + 4);
				lineaParticion = string_new();
				string_append(&lineaParticion, "SIZE=0");
				//string_append(&lineaParticion,"1");
				string_append(&lineaParticion, "\n");
				string_append(&lineaParticion, "BLOCK=[");
				string_append(&lineaParticion, "1");
				string_append(&lineaParticion, "]");
				fputs(lineaParticion, particion);
				log_info(logger, "Particion creada: %s", archivoParticion);
				fclose(particion);

			}

			// FALTA ASIGNAR BLOQUE PARA LA PARTICION
			return 0;
		} else {
			log_info(logger, "No se pudo crear el archivo metadata \n");
			fclose(archivoMetadata);
			return -1;
		}
	} else {
		log_info(logger, "La tabla ya existe \n");
		perror("La tabla ingresada ya existe");
		return -2;

	}

}

int comandoInsertSinTimestamp(char* tabla, char* key, char* value) {

	int aux = time(NULL);

	char timestamp[11];

	sprintf(timestamp, "%d", aux);

	log_info(logger, "el timestamp a agregar es: %s", timestamp);

	return comandoInsert(tabla, key, value, timestamp);

}

int comandoInsert(char* tabla, char* key, char* value, char* timestamp) {
	int retornoVerificar = verificarTabla(tabla);
	if (retornoVerificar == 0) {

		bool verificarValue = validarValue(value);
		bool verificarKey = validarKey(key);
		bool algunoContiene = (verificarValue || verificarKey);

		if (!algunoContiene) {

			char* valueDesenmascarado = desenmascararValue(value);

			t_registroMemtable* registroPorAgregarE = armarEstructura(
					valueDesenmascarado, key, timestamp);

			// Verifico que la key ya exista en el memtable, aca se hace el dump
			mutexBloquear(&memtable_mx);
			bool tablaRepetida = dictionary_has_key(memtable, tabla);
			mutexDesbloquear(&memtable_mx);
			log_info(logger, "valor tablaRepetida %d", tablaRepetida);

			if (tablaRepetida) {
				log_info(logger, "Encontre una tabla repetida");
				mutexBloquear(&memtable_mx);
				t_list* tableRegisters = dictionary_get(memtable, tabla);
				list_add(tableRegisters, registroPorAgregarE);
				mutexDesbloquear(&memtable_mx);

			} else {

				t_list* listaAux = list_create();
				list_add(listaAux, registroPorAgregarE);

				mutexBloquear(&memtable_mx);
				dictionary_put(memtable, tabla, listaAux);
				mutexDesbloquear(&memtable_mx);
				mutexBloquear(&listaTablasInsertadas_mx);
				list_add(listaTablasInsertadas, tabla); //puede llegar a romper, agregar un aux
				mutexDesbloquear(&listaTablasInsertadas_mx);

			}

			/*free(valueDesenmascarado);
			 free(registroPorAgregarE);
			 free(resultado);
			 free(elementoDiccionario);*/
		}

	} else {
		retornoVerificar = -1;

	}
	return retornoVerificar;
}

char* comandoDescribeEspecifico(char* tabla) {

	if (verificarTabla(tabla) == 0) {
		obtenerMetadataTabla(tabla);
		char* resultado = retornarValores(tabla);
		log_info(logger, "resultado describe %s", resultado);
		return resultado;
	} else {
		return NULL;
	}
}

char* comandoDescribe() {

	char* resultado = retornarValoresDirectorio();
	log_info(logger, "resultado describe de todas las tablas %s", resultado);
	return resultado;
}

void cerrarTodo() {

	sem_destroy(&semaforoQueries);
	dictionary_destroy(memtable);
	list_destroy(listaTablasInsertadas);
	fclose(archivoBitmap);
}

