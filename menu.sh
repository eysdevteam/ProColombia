#!/bin/bash
# Bash Menu Script Example
PS3='Select the item: '
options=("Run model*"  "Exit")
select opt in "${options[@]}"
do
    case $opt in
        "Run model*")
             echo "Copiando script de modelo"
             sudo yum -y install git
             sudo git clone https://github.com/eysdevteam/ProColombia.git
             sudo mv /home/centos/ProColombia/modelexec.sh /home/centos/
             sudo chown root:root /home/centos/modelexec.sh
             sudo chmod +x /home/centos/modelexec.sh
             sudo rm -r /home/centos/ProColombia
             sudo bash /home/centos/modelexec.sh

            ;;
        "Exit")
            break
            ;;
        *) echo invalid option;;
    esac
done
