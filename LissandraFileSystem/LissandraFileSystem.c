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
int dumps_a_dividir = 1;

//char* tablaAverificar = NULL; // directorio de la tabla
//char* path_tabla_metadata;
char* archivoParticion;
char* registroPorAgregar;
int primerVoidEsRegistro = 1;


pthread_mutex_t mutex_dump = PTHREAD_MUTEX_INITIALIZER;

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

	/*				PRUEBA MAYOR TIMESTAMP ENTRE 4 REGISTROS

	t_registroMemtable* reg;
	reg = pruebaRegMayorTime();
	return 0;
	*/

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
//	pthread_join(hiloDump, NULL);
	pthread_detach(hiloDump);
	pthread_join(hiloConsola, NULL);
//	pthread_kill(hiloDump, SIGKILL);
	pthread_kill(hiloDump, SIGKILL);
	pthread_kill(hiloServidor, SIGKILL);

//	pthread_join(hiloDump, NULL);
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
	} else {
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

			log_info(logger, "El punto de montaje es: %s",
					configFile->punto_montaje);

			int tamanio = strlen(configFile->punto_montaje) + strlen(TABLE_PATH) + 30;

			tabla_Path = malloc(tamanio);

			//tabla_Path = string_duplicate(configFile->punto_montaje);
//		ARRIBA COMENTADO PARA LIMPIAR LEAKS

			snprintf(tabla_Path,tamanio,"%s%s",configFile->punto_montaje,TABLE_PATH);

			log_info(logger, "Y ahora la variable tabla_path queda con: %s",
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
//			free(archivoCOnfig->properties->elements);	//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig->properties);			//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig->path); 					//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig); 						//AGREGADO PARA LIMPIAR LEAKS
			imprimirVerde(logger,
					"Se ha cargado todos los datos del archivo de configuracion");
			//	log_info(logger, "Se ha cargado todos los datos del archivo de configuracion");
			return true;

		} else {
//			free(archivoCOnfig->properties->elements);	//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig->properties);			//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig->path); 					//AGREGADO PARA LIMPIAR LEAKS
//			free(archivoCOnfig); 						//AGREGADO PARA LIMPIAR LEAKS
			imprimirError(logger,
					"ERROR: No Se han cargado todos o algunos los datos del archivo de configuracion\n");
			//		imprimirMensajeProceso("ERROR: No Se han cargado todos los datos del archivo de configuracion\n");
			return false;
		}

	}
	/*
	free(archivoCOnfig->properties->elements);	//AGREGADO PARA LIMPIAR LEAKS
	free(archivoCOnfig->properties);			//AGREGADO PARA LIMPIAR LEAKS
	free(archivoCOnfig->path); 					//AGREGADO PARA LIMPIAR LEAKS
	free(archivoCOnfig); 						//AGREGADO PARA LIMPIAR LEAKS*/

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
	log_info(logger,"Voy a cargar bitmap");

//	bitmapPath = malloc(sizeof(char) * 50);
	int tamanio = strlen(configFile->punto_montaje)+strlen(PATH_LFILESYSTEM_BITMAP)+1;
	bitmapPath = malloc(tamanio);
	snprintf(bitmapPath, tamanio, "%s%s",configFile->punto_montaje, PATH_LFILESYSTEM_BITMAP);
//	strcpy(bitmapPath, "");
//	bitmapPath = string_new();	--COMENTADO PARA LIMPIAR LEAKS
	log_info(logger,"Path del bitmap: %s",bitmapPath);
	/*string_append(&bitmapPath, configFile->punto_montaje);

	string_append(&bitmapPath, PATH_LFILESYSTEM_BITMAP);*/

	if (!existeArchivo(bitmapPath)) {
		log_info(log,
				"Archivo de bitmap no existe, se procede a crear el bitmap");
		bitarray = crearBitarray();
	} else {
		log_info(logger, "existe archivo, se procede a abrir el bitmap");
		abrirBitmap();
	}

	log_info(logger, "cantidad de bloques libres en el bitmap: %d",
			cantBloquesLibresBitmap());

	//pruebas de las funciones bitmap
	log_info(logger, "cantidad de bloques ocupados: %d",
			cantidadBloquesOcupadosBitmap());

	ocuparBloqueLibreBitmap(0);
	log_info(logger, "ocupando bloque: %d", 0);
	log_info(logger, "se ocupo bien? tiene que ser 1: %d",
			estadoBloqueBitmap(0));

	log_info(logger, "cantidad de bloques ocupados: %d = 1?",
			cantidadBloquesOcupadosBitmap());
	log_info(logger, "primer bloque libre: %d",
			obtenerPrimerBloqueLibreBitmap());

	liberarBloqueBitmap(0);
	log_info(logger, "okey... vamos a liberarlo");
	log_info(logger, "se libero bien? tiene que ser 0: %d",
			estadoBloqueBitmap(0));

	log_info(logger, "cantidad de bloques ocupados: %d = 0?",
			cantidadBloquesOcupadosBitmap());
}

int abrirBitmap() {

	char* fs_path = string_new();

	string_append(&fs_path, bitmapPath);

	int bitmap = open(fs_path, O_RDWR);
	struct stat mystat;

	if (fstat(bitmap, &mystat) < 0) {
		log_error(logger, "Error en el fstat\n");
		close(bitmap);
	}
//	log_info(logger,"Sin error");
	char *bmap;
	bmap = mmap(NULL, mystat.st_size, PROT_WRITE | PROT_READ, MAP_SHARED,
			bitmap, 0);

//	log_info(logger,"inicialicé mmap");

	if (bmap == MAP_FAILED) {
		log_error(logger, "algo fallo en el mmap");
	}
//	log_info(logger,"Liberaron la metadata y voy a romper");
//	log_info(logger,"voy a crear bitarray para %d bloques",metadataLFS->blocks);
	bitarray = bitarray_create_with_mode(bmap, metadataLFS->blocks / 8,
			MSB_FIRST);

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

	int posicion = 0;

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

		if (linea && strcmp(linea,"\n") && strcmp(linea,"")) {
			add_history(linea);

			sem_wait(&semaforoQueries);
			list_add(list_queries, linea);
			sem_post(&semaforoQueries);
		}

		if (!strncmp(linea, "SALIR", 4)) {
			log_info(logger, "\n\n**************************** SALIENDO ****************************\n\n");
			free(linea);
			break;
		}

		if (linea && strcmp(linea,"\n") && strcmp(linea,"")) { //Así no rompe cuando se apreta enter
			//fgets(bufferComando, MAXSIZE_COMANDO, stdin); -> Implementacion anterior
			comandoSeparado = string_split(linea, separator);

			validarLinea(comandoSeparado, logger);
			int i;
			log_info(logger, "Liberando comando alocado: %s", linea);
			for (i = 0; comandoSeparado[i] != NULL; i++) {
				free(comandoSeparado[i]);
			}
		free(comandoSeparado);
		}
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
			char* resultadoDeDESCRIBE;
			resultadoDeDESCRIBE= comandoDescribe();
			free(resultadoDeDESCRIBE);

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

			t_registroMemtable* reg_aux;

			reg_aux = comandoSelect(comando[1], comando[2]);
			free(reg_aux->value);
			free(reg_aux);
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

	if(comando == NULL){
		log_info(logger, "Recibimos el comando: NULL");
		return -1;
	}

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

	//	char* msg = string_new();    <---------------CREO QUE ESTE ES REDUNDANTE TENIENDO
						//	STRING DUPLICATE(BUFFER) debajo.

		// char * msg = malloc(sizeof(char)*100);
		char* msg = string_duplicate(buffer); // <-- Esto hace funcionar del string por red.

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
		//LIBERO EL ARRAY DE COMANDO, es lo que hace el for y luego el posterior free
		int i;
		for (i = 0; comandoSeparado[i] != NULL; i++) {
			free(comandoSeparado[i]);
		}
		free(comandoSeparado);
		free(buffer);


		//BY DAMIAN: YO dir{ia que si, es un recurso que esta ocupando en memoria.
		//			Pero diria que lo dejen para el final si lo tienen que retornar
		//			a Memoria de alguna forma en el remoto caso de que sea necesario
		free(msg);

	}

}

char *armarPathTabla(char *nombre_tabla)
{
	log_info(logger,"[ARMAR PATH TABLA] tabla recibida %s", nombre_tabla);
	char *nombre_upper = malloc(strlen(nombre_tabla)+1);
	strcpy(nombre_upper,nombre_tabla);
	string_to_upper(nombre_upper);
	int tamanio = strlen(tabla_Path)+strlen(nombre_upper)+1;
	char *path = malloc(tamanio);
	snprintf(path,tamanio,"%s%s",tabla_Path, nombre_upper);
	free(nombre_upper);
	log_info(logger,"[ARMAR PATH TABLA] Path armado %s", path);
	return path;
}

char *armarPathMetadataTabla(char *nombre_tabla)
{
	char *nombre_upper = malloc(strlen(nombre_tabla)+1);
	strcpy(nombre_upper,nombre_tabla);
	string_to_upper(nombre_upper);
	int tamanio = strlen(tabla_Path)+strlen(nombre_upper)+strlen("/metadata")+1;
	char *path = malloc(tamanio);
	snprintf(path,tamanio,"%s%s%s",tabla_Path, nombre_upper, "/metadata");
	log_info(logger,"[ARMAR PATH METADATA TABLA] Path armado %s", path);
	free(nombre_upper);
	return path;
}

int verificarTabla(char *tabla){
	char *path = armarPathTabla(tabla);
	FILE *file;
	log_info(logger,"[VERIFICADO] La direccion de la tabla que se quiere verificar es: %s",path);
	file = fopen(path, "r");
	free(path);

	if (file == NULL) {
		/*free(tablaAverificar);
		tablaAverificar = NULL;*/
		log_error(logger, "[ERROR] No existe la tabla");
		return -1;

	} else {
		/*free(tablaAverificar);
		tablaAverificar = NULL;*/
		log_info(logger, "[ OK ] La tabla ya existe.");
		fclose(file);
		return 0;
	}
}

/*int verificarTabla(char* tabla) {
	// REVISAR LUEGO ESTA FUNCION, esta muy mal hecha

	char* tablaAverificar = NULL; // directorio de la tabla
	char* path_tabla_metadata = NULL;
//	if(tablaAverificar!=NULL){
//		free(tablaAverificar);
//		tablaAverificar = NULL;
//	}
	int tamanio = strlen(tabla_Path) + strlen(tabla) + 1;
	tablaAverificar = malloc(tamanio);

	log_info(logger,"Se reservo memoria para contatenar punto de montaje con la tabla");
//	tablaAverificar = string_new();

	for (int i = 0; i < strlen(tabla); i++) {
		tabla[i] = toupper(tabla[i]);
	}
	snprintf(tablaAverificar, tamanio, "%s%s", tabla_Path, tabla);
//	string_append(&tablaAverificar, tabla_Path);
//	string_append(&tablaAverificar, tabla);
	log_info(logger, "Concatenamos: %s a tablaAVerificar", tabla);
	log_info(logger,"[VERIFICADO] La direccion de la tabla que se quiere verificar es: %s",tablaAverificar);

//	path_tabla_metadata = string_duplicate(tablaAverificar);
//	path_tabla_metadata = malloc(strlen(tablaAverificar)+strlen("/metadata")+1);
//	sprintf(path_tabla_metadata, "%s%s", tablaAverificar, "/metadata");

//	string_append(&path_tabla_metadata, "/metadata");

	FILE *file;

	file = fopen(tablaAverificar, "r");

	if (file == NULL) {
		//free(tablaAverificar);
		//tablaAverificar = NULL;
		log_error(logger, "[ERROR] No existe la tabla");
		return -1;

	} else {
		//free(tablaAverificar);
		//tablaAverificar = NULL;
		log_info(logger, "[ OK ] La tabla ya existe.");
		fclose(file);
		return 0;
	}

}*/

t_metadata_tabla* obtenerMetadataTabla(char* tabla) {

	t_metadata_tabla* metadataTabla;

	char *path_tabla_metadata = armarPathMetadataTabla(tabla);
	log_info(logger, "[obtenerMetadata] (+) metadata a abrir : %s",
			path_tabla_metadata);

	int result = 0;

	metadataTabla = malloc(sizeof(t_metadata_tabla)); // Vatiable global.--->la hago local para soportar varios procesos concurrentes

	t_config* metadataFile;
	metadataFile = config_create(path_tabla_metadata);

	if (metadataFile != NULL) {

		log_info(logger, "LFS: Leyendo metadata...");

		if (config_has_property(metadataFile, "CONSISTENCY")) {

			log_info(logger, "Almacenando consistencia");
			// PROBLEMA.
			metadataTabla->consistency = config_get_string_value(metadataFile,
					"CONSISTENCY");

			log_info(logger, "La consistencia  es: %s", metadataTabla->consistency);

		} else {

			log_error(logger, "El metadata no contiene la consistencia");

		} // if (config_has_property(metadataFile, "CONSISTENCY"))

		if (config_has_property(metadataFile, "PARTITIONS")) {

			log_info(logger, "Almacenando particiones");

			metadataTabla->particiones = config_get_int_value(metadataFile,
					"PARTITIONS");

			log_info(logger, "Las particiones son : %d", metadataTabla->particiones);

		} else {

			log_error(logger, "El metadata no contiene particiones");

		} // if (config_has_property(metadataFile, "PARTITIONS"))

		if (config_has_property(metadataFile, "COMPACTION_TIME")) {

			metadataTabla->compaction_time = config_get_int_value(metadataFile,
					"COMPACTION_TIME");

			log_info(logger, "el tiempo de compactacion es: %d",
					metadataTabla->compaction_time);

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

	free(metadataFile->path);
	int i = 0;
	t_hash_element* aux_element;
	while(i < metadataFile->properties->elements_amount){
		while(metadataFile->properties->elements[i]!=NULL){
			aux_element = metadataFile->properties->elements[i]->next;
			free(metadataFile->properties->elements[i]->data);
			free(metadataFile->properties->elements[i]->key);
			metadataFile->properties->elements[i] = aux_element;
		}
//		free(metadataFile->properties->elements[i]);
		i++;
	}

	free(path_tabla_metadata);

	free(metadataFile->properties);
	free(metadataFile);

	//log_info(logger, "[obtenerMetadata] (-) metadata a abrir : %s",tablaAverificar);

	if(result == -1){
		free(metadataTabla);
		return NULL;
	}

	return metadataTabla;

}

int obtenerMetadata() {

	log_info(logger, "levantando metadata del File System");

	int result = 0;

	metadataLFS = malloc(sizeof(t_metadata_LFS)); // Vatiable global.

	int tamanio = strlen(configFile->punto_montaje) + strlen(PATH_LFILESYSTEM_METADATA) + 1;
	char* metadataPath = malloc(tamanio);
//	strcpy(metadataPath, "");       //AGREGADO PARA LIMPIAR LEAKS

	//	metadataPath = string_new();	--COMENTADO PARA LIMPIAR LEAKS

//	string_append(&metadataPath, configFile->punto_montaje);
//	string_append(&metadataPath, PATH_LFILESYSTEM_METADATA);
	snprintf(metadataPath, tamanio, "%s%s", configFile->punto_montaje, PATH_LFILESYSTEM_METADATA);

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

	free(metadataFile->properties->elements);
	free(metadataFile->properties);
	free(metadataFile->path);
	free(metadataFile);
	free(metadataPath);	//AGREGADOS PARA LIMPIEZA DE LEAKS

	log_info(logger, "result: %d", result);

	return result;
}

char* retornarValores(char* tabla, t_metadata_tabla* metadata) {

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
//	valorDescribe = string_new();

	string_append(&valorDescribe, tabla);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, metadata->consistency);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, particiones);
	string_append(&valorDescribe, "|");
	string_append(&valorDescribe, tiempoCompactacion);

	free(particiones);
	free(tiempoCompactacion);
	return valorDescribe;

}

char* retornarValoresDirectorio() {
	DIR *dir;
	struct dirent *ent;
	int tamanio = strlen(configFile->punto_montaje) + strlen(TABLE_PATH) + 1;
	char* pathTabla = malloc(tamanio);
	char* resultado = NULL;
	int memoriaParaMalloc = 0;
	bool encontreAlgoEnDirectorio = false;
//	pathTabla = string_new();
	t_list* lista_describes;
	lista_describes = list_create();

	snprintf(pathTabla, tamanio, "%s%s", configFile->punto_montaje, TABLE_PATH);
//	string_append(&pathTabla, configFile->punto_montaje);
//	string_append(&pathTabla, TABLE_PATH);

	dir = opendir(pathTabla);

	log_info(logger,"[DEBUG] directorio %s",pathTabla);

	if (dir == NULL) {
		log_error(logger, "No puedo abrir el directorio");
		perror("No puedo abrir el directorio");

	}

	while ((ent = readdir(dir)) != NULL) {

		if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0) {
			log_info(logger, "Tabla analizada= %s", ent->d_name);
			verificarTabla(ent->d_name);
			t_metadata_tabla* metadata = obtenerMetadataTabla(ent->d_name);
			if(metadata != NULL){
				resultado = retornarValores(ent->d_name,metadata);
				log_info(logger, "el resultado es %s", resultado);
				memoriaParaMalloc += strlen(resultado) + 1; // el 1 es por el | para separar cada describe
				log_info(logger, "tamanio malloc es %d", memoriaParaMalloc);
				list_add(lista_describes, resultado);
				encontreAlgoEnDirectorio = true;
				free(metadata);
			}

		}
	}

	log_info(logger,"[DEBUG] Fin de lectura directorio");
	char* resultadoFinal;
	if(encontreAlgoEnDirectorio){
		char* resultadoFinal = malloc(memoriaParaMalloc + 1);
		resultadoFinal = string_new();
		for (int i = 0; i < list_size(lista_describes); i++) {
			log_info(logger,"[DEBUG] Entré al for");
			char* elemento = list_get(lista_describes, i);
			string_append(&resultadoFinal, elemento);
			string_append(&resultadoFinal, "|");
		}
		resultadoFinal = stringTomarDesdeInicio(resultadoFinal,
				strlen(resultadoFinal) - 1);

	}
	if(lista_describes->elements_count > 0){
		t_link_element * aux;
		while(lista_describes->head!=NULL){
			aux = lista_describes->head->next;
			free(lista_describes->head);
			lista_describes->head=aux;
		}
	}

	t_link_element* elemento;
	while(lista_describes->head!=NULL){
		elemento = lista_describes->head->next;
		free(lista_describes->head->data);
		free(lista_describes->head);
		lista_describes->head = elemento;
	}
	if(resultado != NULL)
		free(resultado);
	free(lista_describes);
	free(pathTabla);
	closedir(dir);
	return resultadoFinal;
}

//DUMP

void esperarTiempoDump() {

	while (true) {

		//usleep(configFile->tiempo_dump*1000);
		sleep(15);
		log_info(logger, "Es tiempo de dump, hay cosas en la memtable?");
		mutexBloquear(&listaTablasInsertadas_mx);
		int tam = list_size(listaTablasInsertadas);
		mutexDesbloquear(&listaTablasInsertadas_mx);
		if (tam > 0) {

			pthread_mutex_lock(&mutex_dump);
			log_info(logger, "Se encontraron cosas, se hace el dump");
			realizarDump();
			pthread_mutex_unlock(&mutex_dump);
			cantidad_de_dumps++;
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
	int *bloqueLista;
	log_info(logger,"[DEBUG] Tengo %d bloques libres y necesito %d", cantBloquesLibresBitmap(), cantidad_bloques);
	if (cantBloquesLibresBitmap() >= cantidad_bloques) {
		for (int i = 0; i < cantidad_bloques; i++) {
			bloqueAux = obtenerPrimerBloqueLibreBitmap();
			bloqueLista = malloc(sizeof(int));
			*bloqueLista = bloqueAux;
			if (bloqueAux != -1) {
				ocuparBloqueLibreBitmap(bloqueAux);
				//list_add(bloquesUsados, (void*)bloqueAux);
				list_add(bloquesUsados, bloqueLista);
			} else {
				//liberar los bloques de la lista
				return -1;
			}
		}
	} else {
		log_error(logger,
				"[DUMP] no hay bloques disponibles para hacer el dump");
	}

	void* bufferRegistros = malloc(tam_total_registros);
	bufferRegistros = armarBufferConRegistros(listaRegistrosTabla,
			tam_total_registros);
	int resultadoEscritura = escribirVariosBloques(bloquesUsados,
			tam_total_registros, bufferRegistros);

	if (resultadoEscritura != -1) {
		FILE* temporal;
		temporal = fopen(path, "w");
		log_info(logger, "[DUMP] creamos el archivo, ahora  lo llenamos");
		log_info(logger, "[DUMP] path del archivo %s", path);

		if (temporal != NULL) {
			char* contenido = malloc(
					string_length("SIZE=") + sizeof(char) * 2
							+ string_length("BLOCKS=[]")
							+ sizeof(char) * 2 * list_size(bloquesUsados) - 1); //arreglar para bloques con mas de un numero (string, no char)
			contenido = string_new();
			string_append(&contenido, "SIZE=");
			char* size = malloc(10);
			sprintf(size, "%d", tam_total_registros);
			string_append(&contenido, size);
			string_append(&contenido, "\n");
			string_append(&contenido, "BLOCKS=[");
			char* bloque = malloc(10);
			for (int i = 0; i < list_size(bloquesUsados); i++) {
				int* nroBloque = list_get(bloquesUsados, i);
				sprintf(bloque, "%d", *nroBloque);
				string_append(&contenido, bloque);
				string_append(&contenido, ",");
			}
			contenido = stringTomarDesdeInicio(contenido,
					strlen(contenido) - 1);
			string_append(&contenido, "]");
			fputs(contenido, temporal);
			log_info(logger, "[DUMP] Temporal completado con: %s", contenido);
			fclose(temporal);
		} else {
			//liberar bloques de la lista de bloquesUsados
			log_error(logger,
					"[DUMP] error al abrir el archivo temporal con path: %s",
					path);
		}
	} else {
		log_error(logger,
				"[DUMP] hubo un error al escribir los datos en los bloques");
	}

	return 0;
}

//OPERACIONES CON BLOQUES

int tamTotalListaRegistros(t_list* listaRegistros) {
	int cantidad_registros = list_size(listaRegistros);
	log_info(logger, "cantidad de registros de la lista: %d",
			cantidad_registros);

	int tam_total_registros = 0;
	t_registroMemtable* registro;

	for (int i = 0; i < cantidad_registros; i++) {
		registro = list_get(listaRegistros, i);

		log_info(logger,"[DEBUG] tam registro (%d) = %d",i,registro->tam_registro);

		tam_total_registros += registro->tam_registro - 1; // El -1 porque no estoy escribiendo el \0 al archivo, si no al leer le sobran bytes
	}

	tam_total_registros += sizeof(char) * 3 * cantidad_registros;

	log_info(logger, "tamanio total de registros: %d", tam_total_registros);

	return tam_total_registros;
}

int cuantosBloquesNecesito(int tam_total) {
	log_info(logger,"[CUANTOS BLOQUES NECESITO] Entra tamaño total %d",tam_total);
	log_info(logger,"[CUANTOS BLOQUES NECESITO] El tamaño de los bloques es %d",metadataLFS->block_size);
	if (tam_total % metadataLFS->block_size == 0) {
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
				sizeof(u_int64_t));
		offset += sizeof(u_int64_t);
		memcpy(bufferConRegistros + offset, &punto_y_coma, sizeof(char));
		offset += sizeof(char);
		memcpy(bufferConRegistros + offset, &registro->key, sizeof(u_int16_t));
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

int escribirVariosBloques(t_list* bloques, int tam_total_registros,
		void* buffer) {

	int resultado = 1;
	int offset = 0;
	int tam_total = tam_total_registros;

	for (int i = 0; i < list_size(bloques); i++) {

		log_info(logger, "entro al for");
		int* auxnroBloque = list_get(bloques, i);
		int nroBloque = *auxnroBloque;
		log_info(logger, "numero de bloque %d", nroBloque);
		if (tam_total_registros <= metadataLFS->block_size) {
			log_info(logger, "tam registros menor a block size");
			resultado = escribirBloque(nroBloque, tam_total_registros, offset,
					buffer);
			offset += tam_total_registros;
			tam_total_registros -= tam_total_registros;
		} else {
			log_info(logger, "tam registros mayor a block size");
			resultado = escribirBloque(nroBloque, metadataLFS->block_size,
					offset, buffer);
			tam_total_registros -= metadataLFS->block_size;
			offset += metadataLFS->block_size;
		}

		if (resultado == -1) {
			log_info(logger, "resultado -1");
			//liberar bloques en el bitmap
			break;
		}
		log_info(logger, "salgo del for");
	}

	log_info(logger, "[BLOQUE] Se terminaron de escribir los bloques");

	log_info(logger, "[BLOQUE] Reviso que esté todo bien escrito");

	leerBloquesConsecutivos(bloques,tam_total);

	return resultado;
}

int escribirBloque(int bloque, int size, int offset, void* buffer) {

	char* path = crearPathBloque(bloque);
	log_info(logger, "path de bloque a escribir %s", path);
	FILE* bloqueFile = fopen(path, "wb");

	if (bloqueFile != NULL) {

		log_info(logger, "entre al if");
		fwrite(buffer + offset, size, 1, bloqueFile);
		fclose(bloqueFile);
//		leerBloque(path); //Rompe porque el bloque puede haber quedado cortado
		free(path);
		return 0;
	}

	log_info(logger, "[BLOQUE] bloque %d escrito con exito", bloque);
	free(path);
	return -1;
}

void crearBloques() {

	int tamanio = strlen(configFile->punto_montaje) + strlen(PATH_BLOQUES) + 30;
	char* pathBloque = malloc(tamanio);
	log_info(logger, "Voy a crear los bloques");
	for (int i = 0; i < metadataLFS->blocks; i++) {
		snprintf(pathBloque, tamanio, "%s%sblock%d.bin",configFile->punto_montaje, PATH_BLOQUES, i);
		FILE* bloque;
		bloque = fopen(pathBloque, "a");
		fclose(bloque);

	}
	log_info(logger, "los bloques fueron creados");
	free(pathBloque);
}

char* crearPathBloque(int bloque) {

	int tamanio = strlen(configFile->punto_montaje) + strlen(PATH_BLOQUES) + 30;
	char* pathBloque = malloc(tamanio);
	snprintf(pathBloque, tamanio, "%s%sblock%d.bin", configFile->punto_montaje,
	PATH_BLOQUES, bloque);

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
					sizeof(u_int64_t));
			offset += sizeof(u_int64_t);
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
					+ sizeof(u_int64_t) + sizeof(uint16_t);

			log_info(logger, "timestamp leido: %d", registro->timestamp);
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
	} else {
		log_error(logger, "[BLOQUE] no se pudo leer el archivo con el path: %s",
				path);
	}

	return registros_leidos;
}

int abrirArchivoBloque(FILE **fp, int nroBloque, char *modo) {
	char *pathBloque = crearPathBloque(nroBloque);
	*fp = fopen(pathBloque, modo);
	if (*fp == NULL) {
		log_warning(logger, "[abrirArchivoBloque] Error al abrir el archivo %s",
				pathBloque);
		return -1;
	}
	int tam_bloque;
	fseek(*fp, 0, SEEK_END);
	tam_bloque = ftell(*fp);
	rewind(*fp);
	log_info(logger, "[abrirArchivoBloque] Archivo %s abierto correctamente",
			pathBloque);
	free(pathBloque);
	return tam_bloque;
}

t_list* leerBloquesConsecutivos(t_list *nroBloques, int tam_total) {
	t_list *registros_leidos = list_create();
	FILE *bloque;

	char *aux_value = malloc(configFile->tamanio_value + 1);

	estadoLecturaBloque_t estado = EST_LEER;
	estadoLecturaBloque_t anterior = EST_TIMESTAMP;

	void *aux_campo_leyendo;
	if (configFile->tamanio_value + 1 > sizeof(u_int64_t))
		aux_campo_leyendo = malloc(configFile->tamanio_value + 1);
	else
		aux_campo_leyendo = malloc(sizeof(u_int64_t));
	int offset_bloque = 0, tam_bloque = 0, offset_campo = 0, offset_total = 0,
			bloques_leidos = 0, num_separador = 0;
	bool leiValueEntero;
	t_registroMemtable* registro = malloc(sizeof(t_registroMemtable));
	void* registros_bloque = NULL;

	log_info(logger,
			"[OBTENIENDO TODO BLOQUES] Voy a obtener todos los registros de los bloques indicados");

	while (offset_total < tam_total && estado != EST_FIN) {
//		log_info(logger, "Hasta ahora leí %d bytes de %d", offset_total,
//				tam_total);
		switch (estado) {
		case EST_LEER:
			if (bloques_leidos == list_size(nroBloques)) {
				estado = EST_FIN;
				break;
			}
			offset_bloque = 0;
			int *nBloque = list_get(nroBloques, bloques_leidos);
			log_info(logger,
					"[OBTENIENDO TODO BLOQUES] Leyendo el bloque nro %d, que es el bloque %d",
					bloques_leidos, *nBloque);
			tam_bloque = abrirArchivoBloque(&bloque, *nBloque, "rb");
			log_info(logger,
					"[OBTENIENDO TODO BLOQUES] El tamaño del bloque es: %d",
					tam_bloque);
			if (registros_bloque != NULL)
				free(registros_bloque);
			registros_bloque = malloc(tam_bloque);
			if (fread(registros_bloque, tam_bloque, 1, bloque) == 0) {
				log_info(logger,
						"[OBTENIENDO TODO BLOQUES] Error al leer el bloque %d",
						*nBloque);
				return NULL;
			}
			fclose(bloque);
			log_info(logger,
					"[OBTENIENDO TODO BLOQUES] Bloque leído correctamente");
			bloques_leidos++;
			estado = anterior;
			break;

		case EST_TIMESTAMP:
//			log_info(logger, "Buscando el timestamp");
//			log_info(logger, "Al bloque le quedan %d bytes y yo necesito %d",
//					tam_bloque - offset_bloque, sizeof(u_int64_t) - offset_campo);

			//Si con los bytes que le quedan al bloque me alcanza para completar el campo, los copio y avanzo al siguiente estado
			if (offset_bloque + sizeof(u_int64_t) - offset_campo <= tam_bloque) {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(u_int64_t) - offset_campo);
				memcpy(&(registro->timestamp), aux_campo_leyendo,
						sizeof(u_int64_t));
//				log_info(logger, "Timestamp leido: %d", registro->timestamp);

				//Avanzo los offset los bytes que acabo de leer
				offset_bloque += sizeof(u_int64_t) - offset_campo;
				offset_total += sizeof(u_int64_t) - offset_campo;

				//Avanzo al siguiente estado que es buscar un separador
				anterior = estado;
				estado = EST_SEP;
			}
			//Si no me alcanza, copio todo lo que puedo y voy a leer un nuevo bloque
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						tam_bloque - offset_bloque);

				//Avanzo los offset los bytes que acabo de leer
				offset_campo += tam_bloque - offset_bloque;
				offset_total += tam_bloque - offset_bloque;
				offset_bloque += tam_bloque - offset_bloque;
//				log_info(logger, "Me faltan %d bytes para leer el timestamp",
//						sizeof(u_int64_t) - offset_campo);

				//Voy a leer un nuevo bloque
				anterior = estado;
				estado = EST_LEER;
			}
			break;

		case EST_KEY:
//			log_info(logger, "Buscando key");
//			log_info(logger, "Al bloque le quedan %d bytes y yo necesito %d",
//					tam_bloque - offset_bloque,
//					sizeof(uint16_t) - offset_campo);

			//Si con los bytes que le quedan al bloque me alcanza para completar el campo, los copio y avanzo al siguiente estado
			if (offset_bloque + sizeof(uint16_t) - offset_campo <= tam_bloque) {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(uint16_t) - offset_campo);
				memcpy(&(registro->key), aux_campo_leyendo, sizeof(uint16_t));
//				log_info(logger, "Key leída: %d", registro->key);

				//Avanzo los offset los bytes que acabo de leer
				offset_bloque += sizeof(uint16_t) - offset_campo;
				offset_total += sizeof(uint16_t) - offset_campo;

				//Avanzo al siguiente estado que es buscar un separador
				anterior = estado;
				estado = EST_SEP;
			}
			//Si no me alcanza, copio todo lo que puedo y voy a leer un nuevo bloque
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(uint16_t) - offset_campo);

				//Avanzo los offset los bytes que acabo de leer
				offset_campo += tam_bloque - offset_bloque;
				offset_total += tam_bloque - offset_bloque;
				offset_bloque += tam_bloque - offset_bloque;
//				log_info(logger, "Me faltan %d bytes para leer la key",
//						sizeof(uint16_t) - offset_campo);

				//Voy a leer un nuevo bloque
				anterior = estado;
				estado = EST_LEER;
			}
			break;

		case EST_VALUE:
//			log_info(logger, "Buscando value");
			leiValueEntero = false;

			//Si con los bytes que le quedan al bloque me alcanza para completar el tamaño máximo para un value, los copio y avanzo al siguiente estado
			if (offset_bloque + configFile->tamanio_value - offset_campo
					<= tam_bloque) {
				//log_info(logger, "Voy a copiar a aux %d bytes",configFile->tamanio_value - offset_campo);
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						configFile->tamanio_value - offset_campo);
				memcpy(aux_value, aux_campo_leyendo, configFile->tamanio_value);

				//Como al escribir no se escribe el caracter nulo, al leer lo agrego
				aux_value[configFile->tamanio_value] = '\0';
				leiValueEntero = true;
			}
			//Si no me alcanza, leo todo lo que tenga y me fijo si el value está completo (puede tener menos bytes que el máximo)
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						tam_bloque - offset_bloque);
				memcpy(aux_value, aux_campo_leyendo,
						tam_bloque - offset_bloque);

				//Como al escribir no se escribe el caracter nulo, al leer lo agrego
				aux_value[offset_campo + tam_bloque - offset_bloque] = '\0';

				//Si encuentro el \n, encontré el valor entero y no tengo que leer otro bloque para tener este campo
				if (string_contains(aux_value, "\n"))
					leiValueEntero = true;
				//Si no lo encuentro, sí tengo que leer otro bloque
				else {
					//Avanzo los offsets los bytes que acabo de leer
					offset_campo += tam_bloque - offset_bloque;
					offset_total += tam_bloque - offset_bloque;
					offset_bloque += tam_bloque - offset_bloque;

					//Voy a leer un nuevo bloque
					anterior = estado;
					estado = EST_LEER;
//					log_info(logger,
//							"No encontré el '\\n' y me faltan %d bytes para el máximo del value",
//							configFile->tamanio_value - offset_campo);
				}
			}
			//Si de cualquiera de las 2 formas pude completar el value, lo guardo y avanzo
			if (leiValueEntero) {
				//Corto el string en el \n y lo guardo en el campo value del registro
				char **aux_split = string_split(aux_value, "\n");
				registro->value = malloc(strlen(aux_split[0]) + 1);
				strcpy(registro->value, aux_split[0]);

				//Borro toda la memoria que aloca la función string_split
				int i = 0;
				while (aux_split[i] != NULL) {
					free(aux_split[i]);
					i++;
				}
				free(aux_split);

				//Avanzo los offsets los bytes que acabo de leer
				offset_bloque += strlen(registro->value) - offset_campo;
				offset_total += strlen(registro->value) - offset_campo;

				//Calculo el tamaño
				registro->tam_registro = sizeof(u_int64_t) + sizeof(uint16_t)
						+ strlen(registro->value) + 1;

				//Agrego el nuevo registro a la lista que voy a retornar
				list_add(registros_leidos, registro);
				log_info(logger,
						"[OBTENIENDO TODO BLOQUES] Registro <%d;%d;%s> agregado",
						registro->timestamp, registro->key, registro->value);

				//Avanzo al siguiente estado que es buscar un separador
				anterior = estado;
				estado = EST_SEP;
			}
			break;

		case EST_SEP:
			//log_info(logger, "Buscando un separador");
			//Si no tengo bytes para leer, voy a leer otro bloque
			if (offset_bloque == tam_bloque) {
				anterior = estado;
				estado = EST_LEER;
			}
			//Si tengo un byte, leo y avanzo
			else {
				//No me guardo los separadores porque no los necesito, simplemente avanzo los offsets
				offset_bloque += sizeof(char);
				offset_total += sizeof(char);

				//Voy al siguiente estado, para lo que necesito saber que número de separador estoy leyendo
				anterior = estado;
				switch (num_separador) {
				case 0:
					estado = EST_KEY;
					offset_campo = 0;
					num_separador++;
					break;
				case 1:
					estado = EST_VALUE;
					offset_campo = 0;
					num_separador++;
					break;
				case 2:
					estado = EST_TIMESTAMP;
					offset_campo = 0;
					num_separador = 0;

					//Si voy a leer un timestamp es porque es nuevo registro, por lo que tengo que alocar memoria para guardarlo
					if (offset_total < tam_total)
						registro = malloc(sizeof(t_registroMemtable));
					break;
				}
				//log_info(logger, "Separador leído");
			}
			break;

		}
	}
	log_info(logger, "[OBTENIENDO TODO BLOQUES] Leí todos los bloques");

	//Libero la memoria auxiliar que usé
	free(registros_bloque);
	free(aux_value);
	free(aux_campo_leyendo);

	return registros_leidos;
}

void *imprimirRegistro(t_registroMemtable *reg) {
	log_info(logger, "Registro: <%d;%d;%s>", reg->timestamp, reg->key,
			reg->value);
	return reg;
}

void borrarRegistro(t_registroMemtable *reg) {
	free(reg->value);
	free(reg);
}

int pruebaLecturaBloquesConsecutivos(void) {
	void *buffer = malloc(4000);
	int offset = 0;
	char value[20];
	uint16_t key = 0;
	u_int64_t timestamp = 0;
	char barran[2], coma[2];
	strcpy(barran, "\n");
	strcpy(coma, ",");

	for (int i = 0; i < 200; i++) {
		timestamp += 554;
		key = i % 15;
		snprintf(value, 9, "val%d", i * 13);
		memcpy(buffer + offset, &timestamp, sizeof(u_int64_t));
		offset += sizeof(u_int64_t);
		memcpy(buffer + offset, coma, sizeof(char));
		offset += sizeof(char);
		memcpy(buffer + offset, &key, sizeof(uint16_t));
		offset += sizeof(uint16_t);
		memcpy(buffer + offset, coma, sizeof(char));
		offset += sizeof(char);
		memcpy(buffer + offset, value, strlen(value));
		offset += strlen(value);
		memcpy(buffer + offset, barran, sizeof(char));
		offset += sizeof(char);

		log_info(logger, "[PRUEBA] Escribi %d;%d;%s. Hasta ahora son %d bytes",
				timestamp, key, value, offset);
	}

	log_info(logger, "[PRUEBA] En total son %d bytes", offset);

	FILE *bloque1 = NULL;
	FILE *bloque2 = NULL;
	FILE *bloque3 = NULL;
	FILE *bloque4 = NULL;

	int numBloque1 = 2, numBloque2 = 1, numBloque3 = 3, numBloque4 = 113;

	abrirArchivoBloque(&bloque1, numBloque1, "wb");
	abrirArchivoBloque(&bloque2, numBloque2, "wb");
	abrirArchivoBloque(&bloque3, numBloque3, "wb");
	abrirArchivoBloque(&bloque4, numBloque4, "wb");

	int desp, bytes1, bytes2, bytes3, bytes4;
//	aux=offset/2;

	bytes1 = 1000;
	bytes2 = 500;
	bytes3 = 700;
	bytes4 = offset - bytes1 - bytes2 - bytes3;

	desp = 0;

	fwrite(buffer + desp, bytes1, 1, bloque1);
	log_info(logger, "[PRUEBA] Escribí %d bytes en el bloque1", bytes1);
	desp += bytes1;

	fwrite(buffer + desp, bytes2, 1, bloque2);
	log_info(logger, "[PRUEBA] Escribí %d bytes en el bloque2", bytes2);
	desp += bytes2;

	fwrite(buffer + desp, bytes3, 1, bloque3);
	log_info(logger, "[PRUEBA] Escribí %d bytes en el bloque3", bytes3);
	desp += bytes3;

	fwrite(buffer + desp, bytes4, 1, bloque4);
	log_info(logger, "[PRUEBA] Escribí %d bytes en el bloque3", bytes4);
	desp += bytes4;

	fclose(bloque1);
	fclose(bloque2);
	fclose(bloque3);
	fclose(bloque4);

	free(buffer);
	t_list *listaBloques = list_create();

	list_add(listaBloques, &numBloque1);
	list_add(listaBloques, &numBloque2);
	list_add(listaBloques, &numBloque3);
	list_add(listaBloques, &numBloque4);

	t_list *regLeidos = leerBloquesConsecutivos(listaBloques, offset);

	list_iterate(regLeidos, (void*) imprimirRegistro);
	t_registroMemtable *registroMayor;
	list_destroy_and_destroy_elements(regLeidos, (void*) borrarRegistro);

	for (int i = 0; i < 20; i++) {
		log_info(logger,
				"[PRUEBA] Voy a buscar el mayor registro con %d como key", i);
		registroMayor = leerBloquesConsecutivosUnaKey(listaBloques, offset, i,
		false);
		if (registroMayor == NULL) {
			log_error(logger,
					"[PRUEBA] No se pudo leer la lista de bloques indicada");
			continue;
		}
		if (registroMayor->timestamp != 0) {
			log_info(logger, "[PRUEBA] El mayor de la key %d es <%d;%d;%s>", i,
					registroMayor->timestamp, registroMayor->key,
					registroMayor->value);
			printf("\nEl mayor de la key %d es <%d;%d;%s>", i,
					registroMayor->timestamp, registroMayor->key,
					registroMayor->value);
		} else {
			printf("\nNo se encontro registro con la key %d", i);
			log_info(logger, "[PRUEBA] No se encontró registro con la key %d",
					i);
		}
		free(registroMayor->value);
		free(registroMayor);
	}
	list_destroy(listaBloques);
	printf("\n\nSalgo de función de prueba\n\n");
	log_info(logger, "[PRUEBA] Salgo de función de prueba");
	return 1;
}

t_registroMemtable* pruebaRegMayorTime() {

	t_registroMemtable* reg1;
	t_registroMemtable* reg2;
	t_registroMemtable* reg3;
	t_registroMemtable* reg4;
	t_registroMemtable* regMayor;

	reg1 = armarRegistroNulo();
	reg2 = armarRegistroNulo();
	reg3 = armarRegistroNulo();
	reg4 = armarRegistroNulo();

	reg1->timestamp = 10;
	reg2->timestamp = 21;
	reg3->timestamp = 244;
	reg4->timestamp = 10;

	regMayor = tomarMayorRegistro(reg1, reg2, reg3, reg4);

	return regMayor;
}

//OPERACIONES CON BLOQUES

//DUMP
int determinarParticion(int key, int particiones) {

	log_info(logger, "KEY: %d ", key);

	int retornar = key % particiones;

	log_info(logger, "PARTICION: %d ", retornar);

	return retornar;

}

char* rutaParticion(char* tabla, int particion) {
	char *path = armarPathTabla(tabla);
	char * stringParticion = malloc(sizeof(char) * 3);

	sprintf(stringParticion, "%d", particion);

	int tamanio = string_length(path)
			+ string_length(stringParticion) + string_length(PATH_BIN) + 2;
	archivoParticion = malloc(tamanio);

	snprintf(archivoParticion, tamanio, "%s%s%d%s", path, "/",
			particion, PATH_BIN);
	log_info(logger, "La ruta de la particion es: %s", archivoParticion);

	free(path);
	return archivoParticion;
}

/*void escanearParticion(int particion) {

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

}*/

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

	t_metadata_tabla *metadata = obtenerMetadataTabla(tabla);

	if (metadata != NULL) {

		for (int i = 0; i < metadata->particiones; i++) {

			char* archivoParticion = rutaParticion(tabla,i);

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
		free(metadata);

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

	char *path_tabla_metadata = armarPathMetadataTabla(tabla);
	char *path = armarPathTabla(tabla);
	log_info(logger, "Vamos a eliminar el metadata de la tabla: %s",
			path_tabla_metadata);

	int retMet = remove(path_tabla_metadata);

	log_info(logger, "Resultado de remove del metadata de la tabla%d", retMet);

	if (retMet == 0) { // Eliminamos el archivo
		log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
	} else {
		log_info(logger, "No se pudo eliminar el archivo\n");
	}

	log_info(logger, "Vamos a eliminar el directorio: %s", path);

	int retTab = remove(path);

	log_info(logger, "Resultado de remove de la tabla %d", retTab);

	if (retTab == 0) { // Eliminamos el archivo
		log_info(logger, "El archivo fue eliminado satisfactoriamente\n");
	} else {
		log_info(logger, "No se pudo eliminar el archivo\n");
	}

	free(path);
	free(path_tabla_metadata);

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

	int tam_registro = strlen(value) + 1 + sizeof(u_int16_t) + sizeof(u_int64_t); //es un long no un u_int64_t
	registroMemtable->tam_registro = tam_registro;
	registroMemtable->value = malloc(strlen(value)+1);
	strcpy(registroMemtable->value,value);
	u_int64_t timestampRegistro = strtoul(timestamp, NULL, 10);
	registroMemtable->timestamp = timestampRegistro;
	u_int16_t keyRegistro = strtol(key, NULL, 16);
	registroMemtable->key = keyRegistro;

	log_info(logger,"[DEBUG] tamaño de registro agregado = %d", registroMemtable->tam_registro);
	//log_info(logger,"El registro quedo conformado por: \n");
	//log_info(logger,"Value = %s ",registroMemtable->value);
	log_info(logger,"Timestamp = %ld ",registroMemtable->timestamp);
	//log_info(logger,"Key = %x ",registroMemtable->key);
	//log_info(logger,"Se procede a agregar el registro a la memtable");

	return registroMemtable;

}

t_registroMemtable* armarRegistroNulo() {
	t_registroMemtable* aux = malloc(sizeof(t_registroMemtable));
	aux->key = 0;
	aux->tam_registro = 0;
	aux->timestamp = 0;
	aux->value = "asd";

	return aux;
}



t_bloquesUsados* leerTemporaloParticion(char* path) {

	t_config* tempFile;
	tempFile = config_create(path);
	t_bloquesUsados* retVal;

	if (tempFile == NULL) {
		retVal = malloc(sizeof(t_bloquesUsados));
		log_info(logger, "No se ha encontrado el archivo de configuracion");
		retVal->bloques = NULL;
		retVal->size = 0;
	}

	else {
		retVal = malloc(sizeof(t_bloquesUsados));
		log_info(logger, "Se ha encontrado el archivo de configuracion\n");

		int size = config_get_int_value(tempFile, "SIZE");

		log_info(logger, "Size encontrado del temp es: %d", size);

		char** bloques = config_get_array_value(tempFile, "BLOCKS");

		char* aux = bloques[0];
		int i = 0;
		t_list* NroBloques = list_create();
		int* auxNroBloque;

		log_info(logger, "llego al while");

		while (aux != NULL) {

			auxNroBloque = malloc(sizeof(int));
			*auxNroBloque = atoi(aux);
			list_add(NroBloques, auxNroBloque);
			free(aux);
			i++;
			aux = bloques[i];
		}

		log_info(logger, "salgo de	l while");

		retVal->bloques = NroBloques;
		retVal->size = size;

		free(bloques);

	}
	return retVal;
}

t_registroMemtable* obtenerRegistroMayor(char* tabla, int key,
		t_list* listaSegunLugar) {
	log_info(logger, "la key que mande del select %d", key);
	t_registroMemtable* registro = NULL;
	t_registroMemtable* retval = malloc(sizeof(t_registroMemtable));
	for (int i = 0; i < list_size(listaSegunLugar); i++) {
		if (registro == NULL) {

			registro = list_get(listaSegunLugar, i);
			log_info(logger, "key del registro encontrado %d", registro->key);
			if (registro->key != key) {
				log_info(logger,
						"Como es distinta, el registro vuelve a ser NULL");
				registro = NULL;
			} else {
				log_info(logger,
						"[Primer registro obtenido]timestamp del registro obtenido %d",
						registro->timestamp);
				log_info(logger,
						"[Primer registro obtenido]value del registro obtenido %s",
						registro->value);
				log_info(logger,
						"[Primer registro obtenido]key del registro obtenido %d",
						registro->key);
			}

		} else {
			t_registroMemtable* aux = list_get(listaSegunLugar, i);
			if (aux->key == key) {

				log_info(logger,
						"[Ya hay mas de un registro obtenido] timestamp del registro obtenido %d",
						aux->timestamp);
				log_info(logger,
						"[Ya hay mas registro obtenido] value del registro obtenido %s",
						aux->value);
				log_info(logger,
						"[Ya hay mas registro obtenido] key del registro obtenido %d",
						aux->key);

				if (aux->timestamp > registro->timestamp) {
					registro = aux;
					log_info(logger, "[fin] me quede con el de timestamp %d",
							registro->timestamp);
					log_info(logger, "[fin] me quede con el de  value %s",
							registro->value);
				}
			}
		}
	}
	retval->value = malloc(strlen(registro->value)+1);
	retval->key = registro->key;
	retval->tam_registro = registro->tam_registro;
	retval->timestamp = registro->timestamp;
	strcpy(retval->value,registro->value);
//	memcpy(retval,registro,sizeof(t_registroMemtable));


	return retval;
}

t_registroMemtable* registroMayorMemtable(char* tabla, u_int16_t key) {

	t_registroMemtable* registro;
	pthread_mutex_lock(&mutex_dump);

	if (dictionary_has_key(memtable, tabla)) {
		t_list* tableRegisters = dictionary_get(memtable, tabla);
		registro = obtenerRegistroMayor(tabla, key, tableRegisters);

	} else {

		log_info(logger, "voy a armar registro nulo");

		registro = armarRegistroNulo();

		log_info(logger, "arme registro nulo");

		log_info(logger, "Se armo registro nulo, su timestamp es %d",
				registro->timestamp);
	}
	pthread_mutex_unlock(&mutex_dump);
	return registro;
}

t_registroMemtable* registroMayorTemporal(char* tabla, u_int16_t key,
		char* terminacion) { //Terminacion va .tmp o .tmpc

	char *path = armarPathTabla(tabla);
	t_list* temporalesTabla = obtenerArchivosDirectorio(path,
			terminacion);

	free(path);

	t_registroMemtable* registro = malloc(sizeof(t_registroMemtable));
	t_registroMemtable* aux = malloc(sizeof(t_registroMemtable));
	registro = NULL;

	int tamanioLista = list_size(temporalesTabla);
	if (tamanioLista > 0) {
		for (int i = 0; i < tamanioLista; i++) {

			char* pathTemp = armarPathTablaParaDump(tabla, i);
			t_bloquesUsados* lecturaTMP = leerTemporaloParticion(pathTemp);
			if (lecturaTMP->size == 0) { // La idea es que nunca entre aca, si tengo temporal es xq un bloque tengo que tener
				registro = armarRegistroNulo();
			} else {
				if (registro == NULL) {
					registro = leerBloquesConsecutivosUnaKey(
							lecturaTMP->bloques, lecturaTMP->size, key, false);
				} else {
					aux = leerBloquesConsecutivosUnaKey(lecturaTMP->bloques,
							lecturaTMP->size, key, false);
					if (aux->timestamp > registro->timestamp) {
						registro = aux;
					}
				}

			}
		}
	}else{ // La idea es que nunca entre aca, si tengo temporal es xq un bloque tengo que tener
		registro = armarRegistroNulo();
	}
	return registro;
}

t_registroMemtable* registroMayorParticion(char* tabla, u_int16_t key,
		int particiones) {

	t_registroMemtable* registro = malloc(sizeof(t_registroMemtable));
	char* pathParticion = rutaParticion(tabla,particiones);

	log_info(logger, "ruta %s", pathParticion);
	t_bloquesUsados* ListaBloques = leerTemporaloParticion(pathParticion);
	if (ListaBloques->size == 0) {
		registro = armarRegistroNulo();
	} else {
		registro = leerBloquesConsecutivosUnaKey(ListaBloques->bloques,
				ListaBloques->size, key, true);
	}

	return registro;

}

t_registroMemtable* tomarMayorRegistro(t_registroMemtable* reg1,t_registroMemtable* reg2, t_registroMemtable* reg3,t_registroMemtable* reg4) {
	t_registroMemtable* registroMayor = malloc(sizeof(t_registroMemtable));

	log_info(logger, "timestamp reg1: %d", reg1->timestamp);
		log_info(logger, "timestamp reg2: %d", reg2->timestamp);
		log_info(logger, "timestamp reg3: %d", reg3->timestamp);
		log_info(logger, "timestamp reg4: %d", reg4->timestamp);


	if (reg1->timestamp > reg2->timestamp) {
		registroMayor = reg1;
	} else {
		registroMayor = reg2;
	}

	if (reg3->timestamp > reg4->timestamp) {
		if (reg3->timestamp > registroMayor->timestamp) {
			registroMayor = reg3;
		}
	} else {
		if (reg4->timestamp > registroMayor->timestamp){
			registroMayor = reg4;
	}
	}
	log_info(logger, "verificandoFuncion");
	log_info(logger, "timestamp reg1: %d", reg1->timestamp);
	log_info(logger, "timestamp reg2: %d", reg2->timestamp);
	log_info(logger, "timestamp reg3: %d", reg3->timestamp);
	log_info(logger, "timestamp reg4: %d", reg4->timestamp);
	log_info(logger, "el mayor timestamp  %d", registroMayor->timestamp);
	return registroMayor;
}

t_registroMemtable* comandoSelect(char* tabla, char* key) {

	t_registroMemtable* registroMayor;

	if (verificarTabla(tabla) == -1) {
		registroMayor = armarRegistroNulo();
		registroMayor->tam_registro = -1;
		return registroMayor;
	} else { // archivo de tabla encontrado
		t_metadata_tabla *metadata = obtenerMetadataTabla(tabla);
//		int returnObtenerMetadata =

		if (metadata == NULL) {
			registroMayor = armarRegistroNulo();
			registroMayor->tam_registro = -2;
			return registroMayor;
		}; // 0: OK. -1: ERROR. // frenar en caso de error

		int valorKey = atoi(key);
		//u_int16_t valorKeyU = (uint16_t*)key;

		u_int16_t valorKeyU = strtol(key, NULL, 16);

		int particiones = determinarParticion(valorKey, metadata->particiones);

		t_registroMemtable* registroMemtable = registroMayorMemtable(tabla,valorKeyU);

		registroMayor = registroMemtable;

		//t_registroMemtable* registroParticion = registroMayorParticion(tabla,valorKeyU,particiones);
/*
		log_info(logger,"obtuve el primer reg");

		t_registroMemtable* registroParticion = registroMayorTemporal(tabla, valorKeyU,".tmp");

		log_info(logger,"obtuve el segundo reg");

		t_registroMemtable* registroTemporal = registroMayorTemporal(tabla, valorKeyU,".tmp");

		log_info(logger,"obtuve el tercer reg");

		t_registroMemtable* registroTemporalC = registroMayorTemporal(tabla,valorKeyU,".tmpc");

		log_info(logger,"obtuve el cuarto reg");

		registroMayor = tomarMayorRegistro(registroMemtable,registroParticion,registroTemporal,registroTemporalC);

		log_info(logger,"el timestamp mas grande es %ld",registroMayor->timestamp);*/

		printf("Registro obtenido: <%d;%ld;%s>\n", registroMayor->key, registroMayor->timestamp, registroMayor->value);
		printf("Registro obtenido: <%d;%ld;%s>\n", registroMemtable->key, registroMemtable->timestamp, registroMemtable->value);

		free(metadata);
		return registroMayor;

	}
}

int comandoDrop(char* tabla) {

	log_info(logger, "Por verificar tabla");

	int retornoVerificar = verificarTabla(tabla);
	if (retornoVerificar == 0) {
		char*path = armarPathTabla(tabla);

		log_info(logger, "Vamos a eliminar la tabla: %s", path);

		free(path);
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

//		log_info(logger,"%s---%s",tabla,tablaAverificar);
		char *path = armarPathTabla(tabla);

		mkdir(path, 0777);

		log_info(logger, "Se crea la tabla y su direccion es %s ",
				path);
		log_info(logger, "Por crear archivo metadata");

		free(path);

		FILE* archivoMetadata;

		char *path_tabla_metadata = armarPathMetadataTabla(tabla);

		archivoMetadata = fopen(path_tabla_metadata, "w");

		free(path_tabla_metadata);

		if (archivoMetadata != NULL) {
			log_info(logger,
					"El archivo metadata se creo satisfactoriamente\n");
			int tamanioConsistencia = strlen(consistencia) + sizeof("CONSISTENCY=") + 4;
			char *lineaConsistencia = malloc(tamanioConsistencia);

			snprintf(lineaConsistencia,tamanioConsistencia,"CONSISTENCY=%s\n",consistencia);

			log_info(logger, "Se agrego la consistencia %s", lineaConsistencia);

			int tamanioParticiones = strlen(particiones) + sizeof("PARTITIONS=") + 4;
			char *lineaParticiones = malloc(tamanioParticiones);

			snprintf(lineaParticiones,tamanioParticiones,"PARTITIONS=%s\n",particiones);

			log_info(logger, "Se agregaron las particiones %s",lineaParticiones);

			int tamanioTiempoC = strlen(tiempoCompactacion) + strlen("COMPACTION_TIME=") +1;
			char *lineaTiempoCompactacion = malloc(tamanioTiempoC);

			snprintf(lineaTiempoCompactacion,tamanioTiempoC,"COMPACTION_TIME=%s",tiempoCompactacion);

			log_info(logger, "Se agrego el tiempo de compactacion %s",lineaTiempoCompactacion);

			fputs(lineaConsistencia, archivoMetadata);
			fputs(lineaParticiones, archivoMetadata);
			fputs(lineaTiempoCompactacion, archivoMetadata);

			fclose(archivoMetadata);

			log_info(logger, "Por crear particiones");

			int aux = atoi(particiones);

			log_info(logger, "aux=%d", aux);
			for (int i = 0; i < aux; i++) {
				char* archivoParticion = rutaParticion(tabla,i);
				FILE* particion;
				particion = fopen(archivoParticion, "w");

				int tamanio = string_length("SIZE=") + sizeof(int)+ string_length("BLOCKS=[]") + sizeof(int) + 4;
				int bloqueLibre =  obtenerPrimerBloqueLibreBitmap();
				ocuparBloqueLibreBitmap(bloqueLibre);

				char* lineaParticion = malloc(tamanio);

				snprintf(lineaParticion,tamanio,"SIZE=0\nBLOCKS=[%d]",bloqueLibre);

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

	u_int64_t aux = timestamp();

	char timestamp_s[30];

	sprintf(timestamp_s, "%ld", aux);

	log_info(logger, "el timestamp a agregar es: %s", timestamp_s);

	return comandoInsert(tabla, key, value, timestamp_s);

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


				log_info(logger,"[DEBUG] agregué registro a la memtable <%d;%s>",registroPorAgregarE->key,registroPorAgregarE->value);
				log_info(logger,"[DEBUG] esta tabla en la memtable tiene: ");
				t_list* tableRegisters2 = dictionary_get(memtable, tabla);
				for(int i = 0; i<list_size(tableRegisters2);i++){
					t_registroMemtable * regAux = list_get(tableRegisters2,i);
					log_info(logger,"[DEBUG] %d <%d;%d;%s>",i,regAux->timestamp,regAux->key,regAux->value);
				}


				mutexDesbloquear(&memtable_mx);

			} else {

				t_list* listaAux = list_create();
				list_add(listaAux, registroPorAgregarE);
				char* aux = malloc(strlen(tabla)+1);
				strcpy(aux,tabla);

				mutexBloquear(&memtable_mx);
				dictionary_put(memtable, tabla, listaAux);
				mutexDesbloquear(&memtable_mx);
				mutexBloquear(&listaTablasInsertadas_mx);
				list_add(listaTablasInsertadas, aux); //puede llegar a romper, agregar un aux
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

	t_metadata_tabla *metadata;
	if (verificarTabla(tabla) == 0) {
		metadata = obtenerMetadataTabla(tabla);
		if(metadata != NULL){
			char* resultado = retornarValores(tabla, metadata);
			free(metadata);
			log_info(logger, "resultado describe %s", resultado);
			return resultado;
		}
		else{
			return NULL;
		}
	} else {
		return NULL;
	}
}

char* comandoDescribe() {

	char* resultado = retornarValoresDirectorio();
	log_info(logger, "resultado describe de todas las tablas %s", resultado);
	return resultado;
}

t_list *obtenerArchivosDirectorio(char *path, char *terminacion) {
	t_list *retval = list_create();

	struct dirent *de;  // Pointer for directory entry
	// opendir() returns a pointer of DIR type.
	DIR *dr = opendir(path);
	;

	if (dr == NULL)  // opendir returns NULL if couldn't open directory
	{
		return NULL;
	}
	char *path_completo;
	int size;
	while ((de = readdir(dr)) != NULL) {
		if (string_ends_with(de->d_name, terminacion)) {
			size = strlen(path) + strlen(de->d_name) + 1;
			path_completo = malloc(size);
			snprintf(path_completo, size, "%s%s", path, de->d_name);
			list_add(retval, path_completo);
		}
	}
	closedir(dr);
	return retval;
}

t_registroMemtable *leerBloquesConsecutivosUnaKey(t_list *nroBloques,
		int tam_total, uint16_t key_buscada, bool es_unica) {
	FILE *bloque;

	t_registroMemtable *registro = malloc(sizeof(t_registroMemtable));
	registro->value = malloc(configFile->tamanio_value + 1);
	strcpy(registro->value, "");
	t_registroMemtable *retval = malloc(sizeof(t_registroMemtable));
	retval->value = malloc(configFile->tamanio_value + 1);
	retval->timestamp = 0;
	strcpy(retval->value, "");

	char *aux_value = malloc(configFile->tamanio_value + 1);

	estadoLecturaBloque_t estado = EST_LEER;
	estadoLecturaBloque_t anterior = EST_TIMESTAMP;

	void *aux_campo_leyendo;
	if (configFile->tamanio_value + 1 > sizeof(u_int64_t))
		aux_campo_leyendo = malloc(configFile->tamanio_value + 1);
	else
		aux_campo_leyendo = malloc(sizeof(u_int64_t));
	int offset_bloque = 0, tam_bloque = 0, offset_campo = 0, offset_total = 0,
			bloques_leidos = 0, num_separador = 0;
	bool leiValueEntero;
	log_info(logger, "[OBTENIENDO KEY BLOQUES] Voy a buscar la key %d",
			key_buscada);
	void* registros_bloque = NULL;
	while (offset_total < tam_total && estado != EST_FIN) {
//		log_info(logger,"Hasta ahora leí %d bytes de %d",offset_total, tam_total);
		switch (estado) {
		case EST_LEER:
			if (bloques_leidos == list_size(nroBloques)) {
				estado = EST_FIN;
				break;
			}
			offset_bloque = 0;
			int *nBloque = list_get(nroBloques, bloques_leidos);
			log_info(logger,
					"[OBTENIENDO KEY BLOQUES] Leyendo el bloque nro %d, que es el bloque %d",
					bloques_leidos, *nBloque);
			tam_bloque = abrirArchivoBloque(&bloque, *nBloque, "rb");
			log_info(logger,
					"[OBTENIENDO KEY BLOQUES] El tamaño del bloque es: %d",
					tam_bloque);
			if (registros_bloque != NULL)
				free(registros_bloque);
			registros_bloque = malloc(tam_bloque);
			if (fread(registros_bloque, tam_bloque, 1, bloque) == 0) {
				log_info(logger,
						"[OBTENIENDO KEY BLOQUES] Error al leer el bloque %d",
						*nBloque);
				return NULL;
			}
			fclose(bloque);
			log_info(logger,
					"[OBTENIENDO KEY BLOQUES] Bloque leído correctamente");
			bloques_leidos++;
			estado = anterior;
			break;

		case EST_TIMESTAMP:
//				log_info(logger, "Buscando el timestamp");
//				log_info(logger, "Al bloque le quedan %d bytes y yo necesito %d",tam_bloque-offset_bloque, sizeof(u_int64_t)-offset_campo);

			//Si con los bytes que le quedan al bloque me alcanza para completar el campo, los copio y avanzo al siguiente estado
			if (offset_bloque + sizeof(u_int64_t) - offset_campo <= tam_bloque) {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(u_int64_t) - offset_campo);
				memcpy(&(registro->timestamp), aux_campo_leyendo,
						sizeof(u_int64_t));
//					log_info(logger, "Timestamp leido: %d",registro->timestamp);

				//Avanzo los offset los bytes que acabo de leer
				offset_bloque += sizeof(u_int64_t) - offset_campo;
				offset_total += sizeof(u_int64_t) - offset_campo;

				//Avanzo al siguiente estado que es buscar un separador
				anterior = estado;
				estado = EST_SEP;
			}
			//Si no me alcanza, copio todo lo que puedo y voy a leer un nuevo bloque
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						tam_bloque - offset_bloque);

				//Avanzo los offset los bytes que acabo de leer
				offset_campo += tam_bloque - offset_bloque;
				offset_total += tam_bloque - offset_bloque;
				offset_bloque += tam_bloque - offset_bloque;
//					log_info(logger, "Me faltan %d bytes para leer el timestamp",sizeof(u_int64_t)-offset_campo);

				//Voy a leer un nuevo bloque
				anterior = estado;
				estado = EST_LEER;
			}
			break;

		case EST_KEY:
//				log_info(logger, "Buscando key");
//				log_info(logger, "Al bloque le quedan %d bytes y yo necesito %d",tam_bloque-offset_bloque, sizeof(uint16_t)-offset_campo);

			//Si con los bytes que le quedan al bloque me alcanza para completar el campo, los copio y avanzo al siguiente estado
			if (offset_bloque + sizeof(uint16_t) - offset_campo <= tam_bloque) {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(uint16_t) - offset_campo);
				memcpy(&(registro->key), aux_campo_leyendo, sizeof(uint16_t));
//					log_info(logger, "Key leída: %d",registro->key);

				//Avanzo los offset los bytes que acabo de leer
				offset_bloque += sizeof(uint16_t) - offset_campo;
				offset_total += sizeof(uint16_t) - offset_campo;

				//Avanzo al siguiente estado que es buscar un separador
				anterior = estado;
				estado = EST_SEP;
			}
			//Si no me alcanza, copio todo lo que puedo y voy a leer un nuevo bloque
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						sizeof(uint16_t) - offset_campo);

				//Avanzo los offset los bytes que acabo de leer
				offset_campo += tam_bloque - offset_bloque;
				offset_total += tam_bloque - offset_bloque;
				offset_bloque += tam_bloque - offset_bloque;
//					log_info(logger, "Me faltan %d bytes para leer la key",sizeof(uint16_t)-offset_campo);

				//Voy a leer un nuevo bloque
				anterior = estado;
				estado = EST_LEER;
			}
			break;

		case EST_VALUE:
//				log_info(logger, "Buscando value");
			leiValueEntero = false;

			//Si con los bytes que le quedan al bloque me alcanza para completar el tamaño máximo para un value, los copio y avanzo al siguiente estado
			if (offset_bloque + configFile->tamanio_value - offset_campo
					<= tam_bloque) {
				//log_info(logger, "Voy a copiar a aux %d bytes",configFile->tamanio_value - offset_campo);
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						configFile->tamanio_value - offset_campo);
				memcpy(aux_value, aux_campo_leyendo, configFile->tamanio_value);

				//Como al escribir no se escribe el caracter nulo, al leer lo agrego
				aux_value[configFile->tamanio_value] = '\0';
				leiValueEntero = true;
			}
			//Si no me alcanza, leo todo lo que tenga y me fijo si el value está completo (puede tener menos bytes que el máximo)
			else {
				memcpy(aux_campo_leyendo + offset_campo,
						registros_bloque + offset_bloque,
						tam_bloque - offset_bloque);
				memcpy(aux_value, aux_campo_leyendo,
						tam_bloque - offset_bloque);

				//Como al escribir no se escribe el caracter nulo, al leer lo agrego
				aux_value[offset_campo + tam_bloque - offset_bloque] = '\0';

				//Si encuentro el \n, encontré el valor entero y no tengo que leer otro bloque para tener este campo
				if (string_contains(aux_value, "\n"))
					leiValueEntero = true;
				//Si no lo encuentro, sí tengo que leer otro bloque
				else {
					//Avanzo los offsets los bytes que acabo de leer
					offset_campo += tam_bloque - offset_bloque;
					offset_total += tam_bloque - offset_bloque;
					offset_bloque += tam_bloque - offset_bloque;

					//Voy a leer un nuevo bloque
					anterior = estado;
					estado = EST_LEER;
//						log_info(logger, "No encontré el '\\n' y me faltan %d bytes para el máximo del value",configFile->tamanio_value-offset_campo);
				}
			}
			//Si de cualquiera de las 2 formas pude completar el value, lo guardo y avanzo
			if (leiValueEntero) {
				//Corto el string en el \n y lo guardo en el campo value del registro
				char **aux_split = string_split(aux_value, "\n");
				strcpy(registro->value, aux_split[0]);

				//Borro toda la memoria que aloca la función string_split
				int i = 0;
				while (aux_split[i] != NULL) {
					free(aux_split[i]);
					i++;
				}
				free(aux_split);

				//Avanzo los offsets los bytes que acabo de leer
				offset_bloque += strlen(registro->value) - offset_campo;
				offset_total += strlen(registro->value) - offset_campo;

				//Calculo el tamaño
				registro->tam_registro = sizeof(u_int64_t) + sizeof(uint16_t)
						+ strlen(registro->value) + 1;

				if (registro->key == key_buscada) {
						//log_info(logger, "[OBTENIENDO KEY BLOQUES] Encontre un registro con la key %d: <%d;%d;%s>", key_buscada, registro->timestamp,registro->key,registro->value);
					if (es_unica) {
//							log_info(logger, "[OBTENIENDO KEY BLOQUES] Al ser único, no sigo buscando");

						retval->key = registro->key;
						retval->tam_registro = registro->tam_registro;
						retval->timestamp = registro->timestamp;
						strcpy(retval->value, registro->value);

						anterior = estado;
						estado = EST_FIN;
					} else {
						if (registro->timestamp > retval->timestamp) {
							retval->key = registro->key;
							retval->tam_registro = registro->tam_registro;
							retval->timestamp = registro->timestamp;
							strcpy(retval->value, registro->value);
						}
						anterior = estado;
						estado = EST_SEP;
					}
				} else {
					anterior = estado;
					estado = EST_SEP;
				}
			}
			break;

		case EST_SEP:
			//log_info(logger, "Buscando un separador");
			//Si no tengo bytes para leer, voy a leer otro bloque
			if (offset_bloque == tam_bloque) {
				anterior = estado;
				estado = EST_LEER;
			}
			//Si tengo un byte, leo y avanzo
			else {
				//No me guardo los separadores porque no los necesito, simplemente avanzo los offsets
				offset_bloque += sizeof(char);
				offset_total += sizeof(char);

				//Voy al siguiente estado, para lo que necesito saber que número de separador estoy leyendo
				anterior = estado;
				switch (num_separador) {
				case 0:
					estado = EST_KEY;
					offset_campo = 0;
					num_separador++;
					break;
				case 1:
					estado = EST_VALUE;
					offset_campo = 0;
					num_separador++;
					break;
				case 2:
					estado = EST_TIMESTAMP;
					offset_campo = 0;
					num_separador = 0;
					break;
				}
				//log_info(logger, "Separador leído");
			}
			break;

		}
	}
	log_info(logger, "[OBTENIENDO KEY BLOQUES] Terminé de leer los bloques");

	//Libero la memoria auxiliar que usé
	if (registros_bloque != NULL)
		free(registros_bloque);
	free(aux_value);
	free(aux_campo_leyendo);

	log_info(logger, "libere bloque no nulo");

	if (registro != NULL) {
		if (registro->value != NULL)
			free(registro->value);
		free(registro);
	}
	log_info(logger, "por imprimir el retval");
	if (retval->timestamp != 0) {
		log_info(logger, "antes de imprimir");
		log_info(logger, "antes de imprimir %d",retval->key);
		log_info(logger, "antes de imprimir %d",retval->timestamp);
		log_info(logger, "antes de imprimir %s",retval->value);

		//log_info(logger,"[OBTENIENDO KEY BLOQUES] El registro con mayor timestamp es <%d;%d;%s>",retval->timestamp, retval->key, "retval->value");
	} else {
		log_info(logger,
				"[OBTENIENDO KEY BLOQUES] No encontré un registro con la key indicada");
	}
	return retval;
}

void cerrarTodo() {
	liberarTodosLosRecursosGlobalesQueNoSeCerraron();
	sem_destroy(&semaforoQueries);
	dictionary_destroy(memtable);
	list_destroy(listaTablasInsertadas);
	fclose(archivoBitmap);
}

void liberarTodosLosRecursosGlobalesQueNoSeCerraron() {
	free(metadataLFS);
	free(bitmapPath);
	free(tabla_Path);
//	free(path_tabla_metadata);
}

