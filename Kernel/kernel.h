/*
 * kernel.h
 *
 *  Created on: 4 abr. 2019
 *      Author: utnso
 */

#ifndef KERNEL_H_
#define KERNEL_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/collections/queue.h>
#include "../Biblioteca/src/Biblioteca.c"
#include <readline/readline.h>
#include <readline/history.h>
#include <pthread.h>


#define PATH_KERNEL_CONFIG "KERNEL.txt"
#define LOG_PATH "logKERNEL.txt"

int socket_CMemoria;
t_log* log_kernel;

//Socket

int resultado_Conectar, resultado_sendMsj;


typedef struct{

	//Estos son los datos obligatorios que deben estar
	char* ip_memoria;
	int puerto_memoria;
	int quantum; //PARA ALGORITMO ROUND ROBIN
	int multiprocesamiento;
	int metadata_refresh;
	int sleep_ejecucion;
}t_kernel_config;

t_kernel_config* arc_config;

typedef struct {
	AddrInfo informacion;
	SockAddrIn address;
	Socklen tamanioAddress;
	String port;
	String ip;
}conexion;

conexion* estructuraConexion;

/*Elementos de consola*/
#define MAXSIZE_COMANDO 200
enum {Select, insert, create, describe, drop, journal, add,run, salir};
char* linea;
void consola();
void menu();

void cargarConfiguracion();

void conexionKernel();


#endif /* KERNEL_H_ */