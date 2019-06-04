#include "comunicacion.h"

/*
 * Ejemplo de servidor utilizando la biblioteca "comunicacion.h"
 */

void * atenderCliente(int * socket_p);

int g_n_cliente = 0;

#ifdef TEST_SERVIDOR
//Recibe IP y PUERTO para conectarse como argumentos. Espera un cliente. Cuando lo recibe, envia y recibe mensajes
int main(int argc, char **argv)
{
    char*ip,*puerto;
    int server_fd;
    pthread_t thread_cliente;
    cliente_com_t cliente;
    if(argc<3){
		printf("\nSe deben indicar como argumentos del main ip y puerto\n\n");
		return -1;
	}
    ip = argv[1];
    puerto = argv[2];

    //Creo dos mensajes de handshake. Uno para los clientes que reciba y otro para los que rechace
    handshake_com_t hs_bienvenido, hs_rechazado;

    //Mensaje de bienvenida que voy a enviar a los clientes que reciba
    hs_bienvenido.id = MEMORIA;
    hs_bienvenido.msg.tam = 40;
    hs_bienvenido.msg.str = malloc(hs_bienvenido.msg.tam);
    strcpy(hs_bienvenido.msg.str,"Bienvenido!");

    //Mensaje de rechazo de conexión que voy a enviar a los clientes que no pueda recibir
    hs_rechazado.id = RECHAZADO;
    hs_rechazado.msg.tam = 40;
    hs_rechazado.msg.str = malloc(hs_rechazado.msg.tam);
    strcpy(hs_rechazado.msg.str,"No te puedo recibir...");

    printf("\n**Preparando para escuchar puerto: %s. Ip: %s**\n",puerto,ip);

    //Levanto el servidor
    do{
         server_fd = iniciar_servidor(ip,puerto);
         if( server_fd == -1){
            printf("\n**Error al iniciar el servidor. Volviendo a intentar en 5 segundos**\n");
            sleep(5);
        }
    }while(server_fd == -1 );

    //Esucho el puerto del servidor y voy atendiendo los clientes que llegan
    while(1){
    	//Espero un cliente
		do{
			cliente = esperar_cliente(server_fd);
			if( cliente.socket == -1){
				printf("\n**No hay clientes intentando conectarse. Volviendo a intentar en 5 segundos**\n");
				sleep(5);
			}
		}while(cliente.socket == -1);
		printf("\nSe conecto un cliente!");

		//Chequeo que el cliente que se conectó sea un cliente que puedo recibir
		printf("\nTipo cliente: %d",cliente.id);
		switch(cliente.id){
			case KERNEL:
				printf("\nSe conectó el kernel. Lo voy a atender!");
				//Le envío mensaje de bienvenida, informando quien soy
				enviar_handshake(cliente.socket,hs_bienvenido);
				//Comienzo a atenderlo
				pthread_create(&thread_cliente,NULL,(void*)atenderCliente,(void *)&(cliente.socket));
				pthread_detach(thread_cliente);
				break;
			case MEMORIA:
				printf("\nSe conectó una memoria. La voy a atender!");
				//Le envío mensaje de bienvenida, informando quien soy
				enviar_handshake(cliente.socket,hs_bienvenido);
				//Comienzo a atenderla
				pthread_create(&thread_cliente,NULL,(void*)atenderCliente,(void *)&(cliente.socket));
				pthread_detach(thread_cliente);
				break;
			default:
				printf("\nNo sé quién se conecto. Voy a rechazar su conexión!");
				//Le envío mensaje de rechazo y cierro su conexión
				enviar_handshake(cliente.socket,hs_rechazado);
				cerrar_conexion(cliente.socket);
				break;
		}
		printf("\n\n");
    }

    //Cierro el socket de escucha de clientes
    cerrar_conexion(server_fd);

    //Libero la memoria que pedí
    borrar_handshake(hs_bienvenido);
    borrar_handshake(hs_rechazado);

    return 0;
}

void * atenderCliente(int * socket_p)
{
	int n_cliente = ++g_n_cliente;
    //int cod_op;
    int fin = 0;
    int conexion = * socket_p;

    //char * msg;
    str_com_t string;
    msg_com_t recibido;
    gos_com_t gossip;
    handshake_com_t hs;
    printf("\nIniciando hilo de lectura!\n\n");
	printf("\n\n-------------------------------------------------------------");
    while(!fin)
    {
    	recibido=recibir_mensaje(conexion);
		printf("\n\n***Cliente <%d>***",n_cliente);
    	switch(recibido.tipo){
    		case DESCONECTADO:
    			fin = 1;
    			break;
    		case GOSSIPING:
    			printf("\n\n***Me llego un pedido de gossiping***");
				gossip = procesar_gossiping(recibido);
				borrar_mensaje(recibido);
				printf("\n%4s %7s%3s%7s", "num", "IP"," ", "PUERTO");
				for(int i=0; i<gossip.cant;i++)
					printf("\n%4d %10s %5s",gossip.seeds[i].numMemoria,gossip.seeds[i].ip,gossip.seeds[i].puerto);
				borrar_gossiping(gossip);
				break;
    		case REQUEST:
    			printf("\n\n***Me llego un request***");
    			string = procesar_request(recibido);
    			borrar_mensaje(recibido);
    			printf("\n\nRequest: %s",string.str);
				borrar_string(string);
    			break;
    		case ERROR:
    			printf("\n\n***Me llego un mensaje de error***");
				string = procesar_error(recibido);
				borrar_mensaje(recibido);
				printf("\n\nMensaje de error: %s",string.str);
				borrar_string(string);
				break;
    		case HANDSHAKE:
    			//No tiene sentido recibir un handshake ahora, sólo es para probar los tipos de mensaje
    			printf("\n\n***Me llego un handshake***");
				hs = procesar_handshake(recibido);
				borrar_mensaje(recibido);
				printf("\n\nIdentificacion: %d. ",hs.id);
				if(hs.msg.tam>0)
					printf("Mensaje: %s",hs.msg.str);
				borrar_handshake(hs);
    			break;
    		default:
    			printf("\nMe llego un mensaje de tipo desconocido...");
    			borrar_mensaje(recibido);
    			break;

    	}
    	printf("\n\n-------------------------------------------------------------");
    }
    printf("\n\n**El cliente <%d> se desconecto. Finalizando...**\n",n_cliente);
    cerrar_conexion(conexion);
}



#endif
