#!/bin/sh

echo "Ingrese la dirección IP de la instancia"
read IP

if 
 ping -c 1 $IP &> /dev/null
 then
	echo "Bajando repositorio ProColombia"

      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo yum -y install git"
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo git clone https://github.com/eysdevteam/ProColombia.git"
      echo "Conectado Mediante SSH - Moviendo Archivos Necesarios para Ejecución"
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo mv /home/centos/ProColombia/DashboardProc /var/www/html/"
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo mv /home/centos/ProColombia/Datos /home/centos/"       
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo mv /home/centos/Datos/model.sh /home/centos/"         
      echo "Permisos de carpeta"
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "chown apache:apache -R /var/www/html/DashboardProc"     
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "chmod 777 -R /var/www/html/DashboardProc"     
      echo "Conectado Mediante SSH - Ejecutando Script"
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo chmod +x /home/centos/model.sh" 
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo chown root:root /home/centos/model.sh"   
      sudo ssh -oStrictHostKeyChecking=no -i /home/centos/cluster_test_biba.pem centos@$IP "sudo bash /home/centos/model.sh"  
      
          
      #salir shell
      echo "terminado"       
 else
 echo "IP invalida"
 exit
fi

exit
