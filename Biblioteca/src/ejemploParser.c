#include "parser.h"

//#define PRUEBA_PARSER

#ifdef PRUEBA_PARSER //Para que no lo compile el eclipse si no defino PRUEBA_PARSER
int main(void)
{
	char *request = readline("\n>>Ingrese un request: ");
	request_t req;
	while(strcmp(request,"fin"))
	{
		//Parseo el string ingresado
		req = parser(request);
		//Imprimo el request completo
		printf("\nIngresó: %s\n",req.request_str);
		//Imprimo el comando del request
		printf("\nCommand: <%d>=<%s>\n",req.command,req.command_str);
		//Recorro el vector de argumentos y los imprimo
		for(int i=0;i<req.cant_args;i++)
			printf("\nArg %d: <%s>\n",i,req.args[i]);
		//Libero la memoria utilizada por el parser
		borrar_request(req);
		//Libero la memoria utilizada por el readline
		free(request);
		request = readline("\n>>Ingrese un request: ");
	}
	free(request);
	return 1;
}
#endif //PRUEBA_PARSER
