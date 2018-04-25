#!/bin/sh


sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/Vicepr/vicepr.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/Jefes/jefes.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/Nacionales/nacionales.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/Internacionales/internacionales.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/General/general.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosProcolombia/Preguntas/preguntas.java


##Donuts
sudo sed -i -e 's/^/[/' /var/www/html/DashboardProc/web/Vicepr/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/DashboardProc/web/Vicepr/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/DashboardProc/web/Jefes/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/DashboardProc/web/Jefes/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/DashboardProc/web/Nacionales/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/DashboardProc/web/Nacionales/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/DashboardProc/web/Internacionales/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/DashboardProc/web/Internacionales/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/DashboardProc/web/General/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/DashboardProc/web/General/donut/donut.json


##Bullets
sudo sed -i '$!s/$/,/' /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/DashboardProc/web/Jefes/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/DashboardProc/web/General/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/DashboardProc/web/General/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/DashboardProc/web/General/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/DashboardProc/web/General/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/DashboardProc/web/General/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/DashboardProc/web/General/bullet/bullet.json

##Preguntas

sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion1/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion1/file1/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion1/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion1/file1/data.json


sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion2/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion2/file1/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion2/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion2/file1/data.json

sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion3/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion3/file1/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion3/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion3/file1/data.json



sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion1/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion1/file2/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion1/file2/data.json
sudo sed -i -e 's|collect_list(valor)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion1/file2/data.json


sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion2/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion2/file2/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion2/file2/data.json
sudo sed -i -e 's|collect_list(valor)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion2/file2/data.json

sudo sed -i '$!s/$/,/'  /var/www/html/DashboardProc/web/Preguntas/seccion3/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/DashboardProc/web/Preguntas/seccion3/file2/data.json
sudo sed -i "\$a]"      /var/www/html/DashboardProc/web/Preguntas/seccion3/file2/data.json
sudo sed -i -e 's|collect_list(valor)|preguntas|g' /var/www/html/DashboardProc/web/Preguntas/seccion3/file2/data.json


exit

