#!/bin/sh

echo "Ingrese la dirección IP de la instancia"
read IP

if 
 ping -c 1 $IP &> /dev/null
 then
	  echo "Copiando proyecto a instancia"      
      sudo scp -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem  /home/centos/ProColombia/* centos@$IP:/home/centos
      echo "Conectado Mediante SSH - Moviendo Archivos Necesarios para Ejecución"
      sudo ssh -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem centos@$IP "sudo mv /home/centos/ProColombia/DashboardProc /var/www/html/"  
      sudo ssh -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem centos@$IP "sudo mv /home/centos/datos/model.sh /home/centos/"    
      echo "Permisos de carpeta"
      sudo ssh -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem centos@$IP "chown apache:apache -R /var/www/html/DashboardProc"     
      sudo ssh -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem centos@$IP "chmod 777 -R /var/www/html/DashboardProc"     
      echo "Conectado Mediante SSH - Ejecutando Script"
      sudo ssh -oStrictHostKeyChecking=no -i /home/ed/cluster_test_biba.pem centos@$IP "bash /home/centos/model.sh"  
      
          
      #salir shell
      echo "terminado"       
 else
 echo "IP invalida"
 exit
fi

exit
