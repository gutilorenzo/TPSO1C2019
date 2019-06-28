/*
 * Gossiping.c
 *
 *  Created on: 21 jun. 2019
 *      Author: martin
 */
#include "Gossiping.h"

time_gos_t ahora(void);

pthread_mutex_t gossip_mutex = PTHREAD_MUTEX_INITIALIZER;
t_list *g_lista_seeds;
t_log *logger_gossiping = NULL;
bool gossiping_inicializado = false;
time_gos_t g_retardo_gos;

void inicializar_estructuras_gossiping(t_log *logger, time_gos_t retardo)
{
	if(gossiping_inicializado == false){
		logger_gossiping = logger;
		g_lista_seeds = list_create();
		g_retardo_gos = retardo;
		gossiping_inicializado = true;
	}
}

void actualizar_retardo_gossiping(time_gos_t retardo)
{
	g_retardo_gos = retardo;
}

void liberar_memoria_gossiping(void)
{
	list_clean_and_destroy_elements(g_lista_seeds,borrar_seed);
	gossiping_inicializado = false;
}

void agregar_seed(int nro_mem, char* ip, char *puerto)
{
	seed_com_t *aux = malloc(sizeof(seed_com_t));
	aux->numMemoria = nro_mem;
	strcpy(aux->ip,ip);
	strcpy(aux->puerto,puerto);
	list_add(g_lista_seeds,aux);
}

void incorporar_seeds_gossiping(gos_com_t nuevas)
{
	seed_com_t *nueva_aux;
	int i_mem;
	for(int i=0; i<nuevas.cant; i++){
		i_mem = conozco_memoria(nuevas.seeds[i]);
		if(i_mem == -1){
			nueva_aux = malloc(sizeof(seed_com_t));
			nueva_aux->numMemoria = nuevas.seeds[i].numMemoria;
			strcpy(nueva_aux->ip,nuevas.seeds[i].ip);
			strcpy(nueva_aux->puerto,nuevas.seeds[i].puerto);
			list_add(g_lista_seeds,nueva_aux);
			log_info(logger_gossiping,"[INCORPORAR SEEDS] Nueva memoria conocida. %d-%s-%s",nuevas.seeds[i].numMemoria,nuevas.seeds[i].ip,nuevas.seeds[i].puerto);
		}
		else{
			seed_com_t *aux = list_get(g_lista_seeds,i_mem);
			if(aux->numMemoria == -1 && nuevas.seeds[i].numMemoria != -1){
				aux->numMemoria = nuevas.seeds[i].numMemoria;
				log_info(logger_gossiping,"[INCORPORAR SEEDS] Memoria de alta en el pool. %d-%s-%s\n",nuevas.seeds[i].numMemoria,nuevas.seeds[i].ip,nuevas.seeds[i].puerto);
			}
			else{
				log_info(logger_gossiping,"[INCORPORAR SEEDS] Memoria ya conocida. %d-%s-%s\n",nuevas.seeds[i].numMemoria,nuevas.seeds[i].ip,nuevas.seeds[i].puerto);
			}
		}
	}
}

gos_com_t armar_vector_seeds(id_com_t id_proceso)
{
	gos_com_t gos_tabla;
	seed_com_t *aux;
	gos_tabla.cant = list_size(g_lista_seeds);
	gos_tabla.seeds = malloc(sizeof(seed_com_t)*gos_tabla.cant);
	int activas = 0;
	for(int i=0; i<gos_tabla.cant;i++)
	{
		aux = list_get(g_lista_seeds,i);
		if(id_proceso == KERNEL && aux->numMemoria == -1)
			continue;
		gos_tabla.seeds[activas] = *aux;
		activas++;
	}
	if(activas < gos_tabla.cant){
		gos_tabla.cant = activas;
		gos_tabla.seeds = realloc(gos_tabla.seeds,sizeof(seed_com_t)*gos_tabla.cant);
	}
	return gos_tabla;
}

int conozco_memoria(seed_com_t memoria)
{
	seed_com_t *aux;
	for(int i=0; i<list_size(g_lista_seeds);i++){
		aux = list_get(g_lista_seeds,i);
		if(strcmp(aux->ip,memoria.ip)==0 && strcmp(aux->puerto,memoria.puerto)==0)
			return i;
	}
	printf("No la conozco\n");
	return -1;
}

void borrar_seed(seed_com_t *memoria)
{
	seed_com_t *aux;
	for(int i=0; i<list_size(g_lista_seeds);i++){
		aux = list_get(g_lista_seeds,i);
		if(strcmp(aux->ip,memoria->ip)!=0 && strcmp(aux->puerto,memoria->puerto)!=0){
			log_info(logger_gossiping,"Seed %d-%s-%s borrado",memoria->numMemoria,memoria->ip,memoria->puerto);
			list_remove(g_lista_seeds,i);
			break;
		}
	}
}

void registrar_memoria_caida(int i_mem)
{
	seed_com_t *aux = list_get(g_lista_seeds,i_mem);
	aux->numMemoria = -1;
}

void correr_gossiping(id_com_t id_proceso)
{
	imprimirMensaje(logger_gossiping,"[CORRIENDO GOSSIPING] Iniciando proceso de gossiping");

	handshake_com_t hs;
	msg_com_t msg;
	gos_com_t nuevas;

	pthread_mutex_lock(&gossip_mutex);
	//Me guardo las memorias conocidas hasta el momento. Son con las que haré gossiping ahora
	t_list *copia_seeds = list_duplicate(g_lista_seeds);

	//Me armo un vector con las memorias conocidas que presentaré a las demás memorias
	gos_com_t conocidas = armar_vector_seeds(id_proceso);

	for(int i=1; i<list_size(copia_seeds); i++){ //La primera memoria siempre soy yo mismo
		seed_com_t *memoria = list_get(copia_seeds,i);

		//Me conecto a la memoria
		int conexion = conectar_a_servidor(memoria->ip,memoria->puerto,id_proceso);
		if(conexion==-1){
			log_info(logger_gossiping,"La memoria <%d>-<%s>-<%s> está caida",memoria->numMemoria,memoria->ip,memoria->puerto);
			//Tengo que borrarla de la lista
			//borrar_seed(memoria);
			registrar_memoria_caida(i);
			continue;
		}

		//Recibo el hs de la memoria para ver si me acepta
		msg = recibir_mensaje(conexion);
		if(msg.tipo != HANDSHAKECOMANDO){
			log_info(logger_gossiping,"La memoria no responde como se espera");
			close(conexion);
			continue;
		}

		hs = procesar_handshake(msg);
		borrar_mensaje(msg);
		if(hs.id == RECHAZADO){
			log_info(logger_gossiping,"La memoria rechazo la conexion");
			close(conexion);
			continue;
		}
		borrar_handshake(hs);
		log_info(logger_gossiping,"La memoria acepto la conexion");

		//Le envío las memorias que conozco
		enviar_gossiping(conexion,conocidas);

		//Espero su respuesta
		msg = recibir_mensaje(conexion);
		if(msg.tipo != GOSSIPING){
			log_info(logger_gossiping,"La memoria no responde como se espera");
			close(conexion);
			continue;
		}

		nuevas = procesar_gossiping(msg);
		borrar_mensaje(msg);

		incorporar_seeds_gossiping(nuevas);
		log_info(logger_gossiping,"Se agregaron memorias recibidas");
		borrar_gossiping(nuevas);

		close(conexion);
	}
	borrar_gossiping(conocidas);
	pthread_mutex_unlock(&gossip_mutex);
	list_destroy(copia_seeds);
}

int iniciar_hilo_gossiping(id_com_t *id_proceso, pthread_t *thread)
{
	if(gossiping_inicializado == false){
		imprimirAviso(logger_gossiping,"[INICIANDO HILO GOSSIPING] Debe inicializar las variables de gossiping. No se lanzará el hilo");
		return -1;
	}
	imprimirMensaje(logger_gossiping,"[INICIANDO HILO GOSSIPING] Voy a crear hilo");
	pthread_create(thread,NULL,(void*)hilo_gossiping,id_proceso);
	pthread_detach(*thread);
	imprimirMensaje(logger_gossiping,"[INICIANDO HILO GOSSIPING] Hilo creado");
	return 1;
}

void *hilo_gossiping(id_com_t * id_proceso)
{
	imprimirMensaje(logger_gossiping,"[HILO GOSSIPING] Entrando a hilo");
	log_info(logger_gossiping,"[HILO GOSSIPING] Soy proceso %d",*id_proceso);
	time_gos_t t0 = ahora(), t1 = 0;
	while(1){
		imprimirMensaje(logger_gossiping,"[HILO GOSSIPING] Voy a correr gossiping");
		correr_gossiping(*id_proceso);
		t1 = ahora();
		imprimirMensaje1(logger_gossiping,"[HILO GOSSIPING] Gossiping ejecutado. El siguiente es en %d segundos",(g_retardo_gos-(t1-t0))/1000);
		usleep((g_retardo_gos-(t1-t0))*1000);
		t0 = ahora();
	}
	return NULL;
}

time_gos_t ahora(void)
{
	time_gos_t retval;
	struct timeval tv;
	if(gettimeofday(&tv,NULL) == -1)
		printf("Error\n");
	retval = tv.tv_sec*1000 + (time_gos_t) tv.tv_usec/1000;
	return retval;
}