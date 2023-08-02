# S3-InfluxDB
Il est important de changer le chemin vers les dossiers contenant les CSV dans la variable ```path_to_data``` du script d'insertion pour que le code soit fonctionnel.

Afin que le code puisse être roulé, il est aussi nécessaire que la base de données soit accessible par le port ```8086```. Un service Windows s'occupe de partir la base de données au démarrage du serveur.

Les cadriciels peuvent être importés avec la commande ````pip install -r requirements.txt```