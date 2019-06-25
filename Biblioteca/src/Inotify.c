/*
 * Inotify.c
 *
 *  Created on: 25 jun. 2019
 *      Author: utnso
 */

#include "Inotify.h"


void inotifyAutomatico(char* pathDelArchivoAEscuchar, int tipo){
	int length, i = 0;
    int fd;
    int wd;
    char buffer[BUF_LEN];
    while(1){

    fd = inotify_init();

    if (fd < 0) {
        perror("inotify_init");
    }

    wd = inotify_add_watch(fd, pathDelArchivoAEscuchar,
        IN_MODIFY | IN_CREATE | IN_DELETE);
    length = read(fd, buffer, BUF_LEN);

    if (length < 0) {
        perror("read");
    }

    while (i < length) {
        struct inotify_event *event =
            (struct inotify_event *) &buffer[i];
        if (event->len) {
            if (event->mask && IN_CREATE) {
                printf("The file %s was created.\n", event->name);
            } else if (event->mask && IN_DELETE) {
                printf("The file %s was deleted.\n", event->name);
            } else if (event->mask && IN_MODIFY) {
                printf("The file %s was modified.\n", event->name);
            }
        }
        i += EVENT_SIZE + event->len;
    }
    printf("\nSe han realizado cambios en %s\n", pathDelArchivoAEscuchar);
    if(tipo==0){
    	//ES MEMORIA
    } else if(tipo==1){
    	//ES KERNEL
    } else {
    	//ES FILESYSTEM
    }
    }
    (void) inotify_rm_watch(fd, wd);
    (void) close(fd);

    return;
}

/*
void inotifyAutomatico(char* pathDelArchivoAEscuchar){
	char buffer[BUF_LEN];
		printf("\nINOTIFY DEL PATH %s\n", pathDelArchivoAEscuchar);
		// Al inicializar inotify este nos devuelve un descriptor de archivo
		int file_descriptor = inotify_init();
		if (file_descriptor < 0) {
			perror("inotify_init");
		}

		// Creamos un monitor sobre un path indicando que eventos queremos escuchar
		int watch_descriptor = inotify_add_watch(file_descriptor,
										pathDelArchivoAEscuchar,
										IN_MODIFY | IN_CREATE | IN_DELETE);

		// El file descriptor creado por inotify, es el que recibe la información sobre los eventos ocurridos
		// para leer esta información el descriptor se lee como si fuera un archivo comun y corriente pero
		// la diferencia esta en que lo que leemos no es el contenido de un archivo sino la información
		// referente a los eventos ocurridos

	int offset = 0;
	while(1) {
		printf("\nEMPIEZO DE NUEVO %s\n", pathDelArchivoAEscuchar);
		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0) {
			printf("NO SE PUDO LEER NADA");
		}


		// Luego del read buffer es un array de n posiciones donde cada posición contiene
		// un eventos ( inotify_event ) junto con el nombre de este.
		while (offset < length) {

		// El buffer es de tipo array de char, o array de bytes. Esto es porque como los
		// nombres pueden tener nombres mas cortos que 24 caracteres el tamaño va a ser menor
		// a sizeof( struct inotify_event ) + 24.
		struct inotify_event *event = (struct inotify_event *) &buffer[offset];


		// El campo "len" nos indica la longitud del tamaño del nombre
		if (event->len) {
			// Dentro de "mask" tenemos el evento que ocurrio y sobre donde ocurrio
			// sea un archivo o un directorio
			if (event->mask && IN_CREATE) {
				if (event->mask && IN_ISDIR) {
					printf("The directory %s was created.\n", pathDelArchivoAEscuchar);
				} else {
					printf("The file %s was created.\n", pathDelArchivoAEscuchar);
				}
			} else if (event->mask && IN_DELETE) {
				if (event->mask && IN_ISDIR) {
					printf("The directory %s was deleted.\n", pathDelArchivoAEscuchar);
				} else {
					printf("The file %s was deleted.\n", pathDelArchivoAEscuchar);
				}
			} else if (event->mask && IN_MODIFY) {
				if (event->mask && IN_ISDIR) {
					printf("The directory %s was modified.\n", pathDelArchivoAEscuchar);
				} else {
					printf("The file %s was modified.\n", pathDelArchivoAEscuchar);
				}
			} else {
				printf("ERROR file reading.\n");
			}
		}
		offset += sizeof (struct inotify_event) + event->len;
		}
	}
	inotify_rm_watch(file_descriptor, watch_descriptor);
	close(file_descriptor);
}

*/
