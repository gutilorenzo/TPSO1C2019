/*
 * main_memoria.c
 *
 *  Created on: 15 jun. 2019
 *      Author: martin
 */

#ifdef COMPILAR_MAIN_MEMORIA
#include "parser.h"
#include "../Biblioteca/src/Biblioteca.h"
#include "memoria.h"

void* hilo_consola(void * args);
char* resolver_pedido(request_t req, int socket_lfs);
char *resolver_select(int socket_lfs,request_t req);
int resolver_insert(request_t req, int modif);

int main(void)
{
	// LOGGING
	printf("INICIANDO EL MODULO MEMORIA \n COMINEZA EL TP PIBE\n\n");
	inicioLogYConfig();

	aux_crear_pagina = malloc(sizeof(pagina));
	aux_devolver_pagina = malloc(sizeof(pagina_a_devolver));
	aux_segmento = malloc(sizeof(segmento));
	aux_tabla_paginas = malloc(sizeof(pagina_referenciada));
	aux_tabla_paginas2 = malloc(sizeof(pagina_referenciada));

	arc_config->max_value_key = 20;
	max_valor_key = arc_config->max_value_key;
	armarMemoriaPrincipal();

	iniciarSemaforosYMutex();

	//ESTE ES SOLO TEST, DESPUES SE BORRA A LA MIERDA
	max_valor_key=6;

	hilo_consola(NULL);

	liberar_todo_por_cierre_de_modulo();


}


void* hilo_consola(void * args){
	request_t req;
	char *linea_leida;
	int fin = 0;
	while(!fin){
		linea_leida=readline(">");
		req = parser(linea_leida);
		free(linea_leida);
		switch(req.command){
			case SALIR:
				fin = 1;
				break;
			case SELECT:
			case INSERT:
				resolver_pedido(req,-1);
				break;
			default:
				printf("\nNO IMPLEMENTADO\n");
				break;
		}
	}
	printf("\nSaliendo de hilo consola\n");
}

char* resolver_pedido(request_t req, int socket_lfs)
{
	char *ret_val=NULL;
	char *ret_ok_generico = malloc(3);
	strcpy(ret_ok_generico,"OK");
	switch(req.command){
		case INSERT:
			imprimirAviso(log_memoria,"\nAVISO, voy a resolver INSERT\n");
			if(resolver_insert(req,true) != -1){
				imprimirAviso(log_memoria,"\nAVISO, INSERT hecho correctamente\n");
				ret_val = ret_ok_generico;
			}
			else{
				imprimirError(log_memoria,"\nERROR, el INSERT no pudo realizarse\n");
			}
			break;
		case SELECT:
			imprimirAviso(log_memoria,"\nAVISO, voy a resolver SELECT\n");
			ret_val = resolver_select(socket_lfs,req);
			if(ret_val != NULL){
				imprimirAviso1(log_memoria,"\nAVISO, SELECT hecho correctamente. Valor %s obtenido\n",ret_val);
			}
			else{
				imprimirError(log_memoria,"\nERROR, el SELECT no pudo realizarse\n");
			}
			break;
		case DROP:
			break;
		case JOURNALCOMANDO:
			break;
		default:
			break;
	}
	if(ret_val != ret_ok_generico){
		free(ret_ok_generico);
	}
	return ret_val;
}


int resolver_drop(char *nombre_tabla, int socket_lfs)
{
	if(!stringEstaVacio(nombre_tabla)){
		imprimirError(log_memoria, "NO SE HA INGRESADO 1 NOMBRE CORRECTO\n");
		return -1;
	}
	if(funcionDrop(nombre_tabla)==-1){
		imprimirError1(log_memoria, "\nERROR, La tabla ya fue eliminada o no existe: <%s>\n", nombre_tabla);
	}

	//Le envio el DROP al filesystem
	req_com_t enviar;
	enviar.tam = strlen("DROP ") + strlen(nombre_tabla) + 1;
	enviar.str = malloc(enviar.tam);
	strcpy(enviar.str,"DROP ");
	strcat(enviar.str,nombre_tabla);
	if(enviar_request(socket_lfs,enviar) == -1){
		imprimirError(log_memoria, "\nERROR, no se puedo enviar el drop al filesystem\n");
	}
	borrar_request_com(enviar);
	//Espero su respuesta
	msg_com_t msg = recibir_mensaje(socket_lfs);
	if(msg.tipo == RESPUESTA){
		resp_com_t recibido = procesar_respuesta(msg);
		borrar_mensaje(msg);
		if(recibido.tipo == RESP_OK){
			imprimirAviso(log_memoria, "\nAVISO, el filesystem realizó el DROP con éxito\n");
		}
		else{
			imprimirError(log_memoria, "\nERROR, el filesystem no pudo realizar el DROP con éxito. ERROR\n");
		}
		if(recibido.msg.tam >0)
			imprimirAviso1(log_memoria,"\nAVISO, el filesystem contestó al DROP con %s\n",recibido.msg.str);
		borrar_respuesta(recibido);
	}
	else{
		borrar_mensaje(msg);
	}
	return 1;
}

int resolver_insert(request_t req, int modif)
{
	if(req.cant_args != 3)
		return -1;
	char *nombre_tabla = req.args[0];
	uint16_t key = atoi(req.args[1]);
	char *valor = req.args[2];
	if(funcionInsert(nombre_tabla, key, valor, modif)== -1){
		imprimirError(log_memoria, "[FUNCION INSERT]\n\nERROR: Mayor al pasar max value\n\n");
		return -1;
	}
	return 1;
}

char* select_memoria(char *nombre_tabla, uint16_t key)
{
	pagina_a_devolver* pagina = malloc(sizeof(pagina_a_devolver));

	segmento *seg;
	int pag, encontrada = 0, aux;
	void* informacion = malloc(sizeof(pagina)+max_valor_key);
	pagina->value = malloc(max_valor_key);

	if(funcionSelect(nombre_tabla, key, &pagina)!=-1){
		pag = buscarEntreLosSegmentosLaPosicionXNombreTablaYKey(
				nombre_tabla,key,&seg,&aux);
		pagina = selectPaginaPorPosicion(pag,informacion);
		printf("\nSEGMENTO <%s>\nKEY<%d>: VALUE: %s\n", nombre_tabla, pagina->key,pagina->value);
		encontrada = 1;
	} else {
		printf("\nERROR <%s><%d>\n", nombre_tabla, key);
	}
	free(informacion);
	if(encontrada){
		return pagina->value;
	}
	return NULL;
}

char *resolver_select(int socket_lfs,request_t req)
{
	char *valor;
	char *nombre_tabla = req.args[0];
	uint16_t key = atoi(req.args[1]);
	valor = select_memoria(nombre_tabla,key);
	if(valor == NULL){
		printf("\nEn memoria no existe la tabla o no existe la key en la misma\n");
		if(socket_lfs != -1){
			//Tengo que pedirlo al filesystem
			req_com_t enviar;
			enviar.tam = strlen(req.request_str)+1;
			enviar.str = malloc(enviar.tam);
			strcpy(enviar.str,req.request_str);
			imprimirAviso(log_memoria,"\nAVISO, voy a mandar select al lfs\n");
			if(enviar_request(socket_lfs,enviar) == -1){
				imprimirError(log_memoria,"\nERROR al conectarse con lfs para enviar select\n");
				return NULL;
			}
			borrar_request_com(enviar);
			imprimirAviso(log_memoria,"\nAVISO, espero respuesta del lfs\n");

			//Espero la respuesta
			msg_com_t msg;
			resp_com_t resp;
			request_t req_lfs;

			msg = recibir_mensaje(socket_lfs);
			imprimirAviso(log_memoria,"\nAVISO, recibi respuesta del lfs\n");
			if(msg.tipo == RESPUESTA){
				resp = procesar_respuesta(msg);
				//ASUMO QUE ME VA A LLEGAR ALGO DEL TIPO: INSERT <TABLA> <KEY> <VALOR> <TIMESTAMP>
				imprimirAviso(log_memoria,"\nAVISO, el lfs contesto\n");
				if(resp.tipo == RESP_OK){
					imprimirAviso(log_memoria,"\nAVISO, el lfs pudo resolver el select con exito");
					req_lfs = parser(resp.msg.str);
					borrar_respuesta(resp);
					resolver_insert(req_lfs,false);
					imprimirAviso(log_memoria,"\nAVISO, agregué el valor a memoria");
					valor = malloc(strlen(req_lfs.args[2])+1);
					strcpy(valor,req_lfs.args[2]);
					borrar_request(req_lfs);
				}
				else{
					imprimirAviso(log_memoria,"\nAVISO, el lfs no pudo resolver el select");
					borrar_respuesta(resp);
				}
			}
			else{
				borrar_mensaje(msg);
			}
		}
	}
	if(valor != NULL){
		printf("\nValor obtenido: %s",valor);
	}
	return valor;
}

void resolver_describe(request_t* req){
	char* nombre;
	if(req->cant_args == 0){
		mutexDesbloquear(&mutex_info_request);
		//ES DESCRIBE DE TODAS LAS COSAS EN MEMORIA
	//	printf("\n\nENTRA AQUI Y AGRADEZCO\n");
		nombre = "";
	//	sleep(5);
		if(funcionDescribe(nombre)==-1){
			printf("\nERROR, NO existe la METADATA de los segmentos\n");
		}
	} else {
		nombre = malloc(strlen(req->args[0]));
		memcpy(nombre, req->args[0], strlen(req->args[0])+1);
		mutexDesbloquear(&mutex_info_request);
		if(funcionDescribe(nombre)==-1){
			printf("\nERROR, NO existe la METADATA de <%s>\n", nombre);
		} else {
			free(nombre);
		}
	}
}

#endif
