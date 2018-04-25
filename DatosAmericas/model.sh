#!/bin/sh

sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/Analistas/analistas.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/Jefes/jefes.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/Directores/directores.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/Gerencias/gerencias.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/General/general.java
sudo $SPARK_HOME/bin/spark-shell -i /home/centos/DatosAmericas/Preguntas/preguntas.java


##Donuts
sudo sed -i -e 's/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Analistas/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/AmericasBps/DashboardABps/web/Analistas/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Jefes/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/AmericasBps/DashboardABps/web/Jefes/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Directores/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/AmericasBps/DashboardABps/web/Directores/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/donut.json

sudo sed -i -e 's/^/[/' /var/www/html/AmericasBps/DashboardABps/web/General/donut/donut.json
sudo sed -i -e 's/$/]/' /var/www/html/AmericasBps/DashboardABps/web/General/donut/donut.json


##Bullets
sudo sed -i '$!s/$/,/' /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/AmericasBps/DashboardABps/web/Analistas/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/AmericasBps/DashboardABps/web/Jefes/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/AmericasBps/DashboardABps/web/Directores/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json

sudo sed -i '$!s/$/,/' /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json
sudo sed -i -e 's|C":5|C":[5]|g' /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json
sudo sed -i -e 's|valor":|valor":[|g' /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json
sudo sed -i -e 's|,"con|],"con|g' /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json
sudo sed -i '1s/^/[/' /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json
sudo sed -i "\$a]" /var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json

##Preguntas

sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/data.json


sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/data.json

sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/data.json
sudo sed -i -e 's|collect_list(name)|preguntas|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/data.json



sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/data.json
sudo sed -i -e 's|collect_list(valor)|valor|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/data.json


sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/data.json
sudo sed -i -e 's|collect_list(valor)|valor|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/data.json

sudo sed -i '$!s/$/,/'  /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/data.json
sudo sed -i '1s/^/[/'   /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/data.json
sudo sed -i "\$a]"      /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/data.json
sudo sed -i -e 's|collect_list(valor)|valor|g' /var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/data.json


exit

