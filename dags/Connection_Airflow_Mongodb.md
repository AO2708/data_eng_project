## Connect to MongoDB on Airflow Apiserver

Admin > Connexions > Ajouter une connexion

Suivez cette configuration :
![Config_premiere_partie](../images/config1_mongo.png)
![Config_seconde_partie](../images/config2_mongo.png)

Si l'option `mongo` pour `Type de Connexion` n'apparait pas, c'est qu'il faut rebuild :

```
docker-compose down
docker-compose up -d --build
```
