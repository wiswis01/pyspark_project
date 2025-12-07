#Installer PySpark
# pip install pyspark

#Créer la SparkSession
from pyspark.sql import SparkSession
#titre de la session :
spark = SparkSession.builder \
    .appName("CrimeAnalysis") \
    .getOrCreate()

#telecharger le fichier nettoyé:
from google.colab import files
files.upload()

#Charger le dataset dans Spark
df_spark = spark.read.csv("crime_clean_pandas.csv", header=True, inferSchema=True)
df_spark.show(5)
df_spark.printSchema()

#filtrer le dataframe pour avoir uniquement des crimes avec une information valide sur l’âge de la victime
from pyspark.sql.functions import col

df_age = df_spark.filter(col("vict_age").isNotNull())

df_age.show(10)

#Combien de crimes disposent d’un âge de victime valide ?

from pyspark.sql.functions import col


df_vict_age = df_spark.filter(
    (col("vict_age").isNotNull()) & (col("vict_age") > 0)#garde seulement les lignes où vict_age n’est pas NULL.
    #enlève les âges 0 ou négatifs (erreurs de saisie).
)

df_vict_age.show(10)      # ACTION 1 : affiche 10 lignes
df_vict_age.count()       # ACTION 2 : nombre de lignes après filtre

#Quels groupes d’âge sont les plus touchés par la criminalité ?
df_crimes_par_age_group = (
    df_vict_age
        .groupBy("vict_age_group")#regroupe les lignes par groupe d’âge
        .count()#compte le nombre de crimes dans chaque groupe.
        .orderBy("count", ascending=False)#trie du groupe le plus touché vers le moins touché.
)

df_crimes_par_age_group.show() #ACTION : voir la répartition des crimes
df_crimes_par_age_group.count() #combien de groupes d’âge différents

from pyspark.sql.functions import col

df_armed = df_spark.filter(
    (col("weapon_desc").isNotNull()) &
    (col("weapon_desc") != "No weapon")
)
#Il reste uniquement les crimes où une arme a été utilisée.
#supprime les lignes où weapon_desc est NULL
#enlève les crimes où weapon_desc == "No weapon"

df_armed.show(5)
df_armed.count()

#Quels types d’armes sont les plus souvent utilisés dans les crimes ?
df_crimes_par_arme = (
    df_armed
        .groupBy("weapon_desc") #regroupe les lignes par weapon_desc
        .count() #compte combien de crimes par type d’arme
        .orderBy("count", ascending=False) #classe du plus fréquent au moins fréquent
)

# ACTION 1
df_crimes_par_arme.show(10) #voir les 10 armes les plus utilisees

# ACTION 2
df_crimes_par_arme.count() #savoir combien de types d’armes différents

#Quelles zones (area_name) enregistrent le plus de crimes ?
df_crimes_par_zone = (
    df_spark
        .filter(col("area_name").isNotNull())
        .groupBy("area_name")
        .count()
        .orderBy("count", ascending=False)
)

#affichier les resultats : Quelles sont les zones les plus touchées par la criminalité ?
df_crimes_par_zone.show(10)
#compter le nombre de zones : Combien de zones différentes sont présentes dans les données?
df_crimes_par_zone.count()