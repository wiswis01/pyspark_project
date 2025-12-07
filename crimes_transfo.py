#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#IMPORTATION DES DONNEES

import pandas as pd
file = "Crime_Data_from_2020_to_Present.csv"
df = pd.read_csv(file)
df


# In[ ]:


#INFORMATION SUR LE DATASET

df.info()  #On remarque 13 variables categorielles et 15 numeriques
#les types dates, heures, latitudes et longitudes de notre dataset ne sont pas coherents et doivent etre uniformiser


# In[ ]:


#DESCRIPTION DES VARIABLES NUMERIQUES DU JEU DE DONNEE

df.describe() #On remarque la variable age avec un minimum negatif ce qui n'es pas logique, et doit par consequent etre transformer sur la bonne echelle


# In[ ]:


#DETECTION DU NOMBRE DE CHAMPS UNIQUES PAR VARIABLE
unique_counts = df.nunique().sort_values(ascending=False)
unique_counts


# In[ ]:


#VERIFICATION DE L'UNICITE DES CHAMPS ET LA PRESENCE DES DOUBLONS
df[df.duplicated(subset=["DR_NO"])]
#en verifiant la presence des doublons dans la cle primaire, on remarque une absence de doublons. ainsi, chaque enregistrement correspond a une valeur unique


# In[ ]:


#DETECTION DES VALEURS MANQUANTES
missing = df.isna().sum().sort_values(ascending=False)
missing
#On remarque que 13 variables contiennent des valeurs manquantes qu'il est indispensable de gerer pour eviter les biais dans l'analyse


# In[ ]:


#Calcul des pourcentages des valeurs manquantes
df.isna().mean().sort_values(ascending=False) * 100


# a l issue de l etape diagnostique, nous avons remarquer des problemes de :
# 
# - erreurs de format ou de types
# - valeurs aberantes
# - valeurs manquantes
# - non unicite de cellule
# - contradictions logiques
# - Categorie incoherente et mal orthographier
# - Problemes d'incoherences semantiques

# # ============================================================
# # NETTOYAGE ET TRANSFORMATION DES DONNEES
# # ============================================================

# # ============================================================
# # TRAITEMENT DES COLONNES STRUCTURELLES (Crm Cd 1 - 4)
# # ============================================================

# In[ ]:


# Liste des colonnes à supprimer
colonnes_a_supprimer = ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]

# Suppression si elles existent
df = df.drop(columns=[c for c in colonnes_a_supprimer if c in df.columns])

# Vérification
print("Colonnes supprimées :", [c for c in colonnes_a_supprimer if c not in df.columns])


# # ============================================================
# # TRAITEMENT DE LA VARIABLE Cross Street (MAR/MNAR)
# # Absence non aléatoire, mais liée au type de localisation
# # ============================================================

# In[ ]:


#suppression de la variable cross street
if "Cross Street" in df.columns:
    df = df.drop(columns=["Cross Street"])
    print("La colonne 'Cross Street' a bien été supprimée.")


# # ============================================================
# # TRAITEMENT DE LA VARIABLE Weapon (MNAR)
# # absence non aleatoire (directement liée au fait qu’aucune arme n’a été employée)
# # ============================================================

# In[ ]:


#TRAITEMENT DE LAVARIABLE Weapon (MNAR)
#Si aucune arme n’est utilisée, la police ne renseigne pas le champ.
#Ainsi, L’absence est directement liée au fait qu’aucune arme n’a été employée. nous avons choisie de remplacer NA par No weapon (catégorie explicite).
#Cela évite d’interpréter l’absence comme une perte d’info aléatoire.
# nettoyer Weapon Used Cd : remplacer NA par 0 = No weapon
if "Weapon Used Cd" in df.columns:
    df["Weapon Used Cd"] = df["Weapon Used Cd"].fillna(0).astype(int)

#nettoyer Weapon Desc : remplacer NA par "No weapon"
if "Weapon Desc" in df.columns:
    df["Weapon Desc"] = df["Weapon Desc"].fillna("No weapon")


df


# # ========================================================================================================
# # TRAITEMENT DE LA COLONNE "MOCODES"
# # ========================================================================================================

# In[ ]:


# ------------------------------------------------------------
# Étape 1 — Nettoyage de la colonne Mocodes
# ------------------------------------------------------------
import numpy as np
if "Mocodes" in df.columns:
    print("=== Nettoyage de Mocodes ===")

    # 1. Normaliser les espaces et supprimer les tabulations
    df["Mocodes"] = df["Mocodes"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # 2. Remplacer les valeurs vides ou incohérentes ("nan", "None") par NaN
    df.loc[df["Mocodes"].isin(["", "nan", "None"]), "Mocodes"] = np.nan    #car les manquants eux meme sont de l'information utile


    print("Étape 1 terminée : Mocodes nettoyé et converti en liste.\n")
    print(df[["Mocodes"]].head())

if "Mocodes" in df.columns:
    df["Mocodes"] = df["Mocodes"].fillna("None")


# # ============================================================
# # TRAITEMENT DES VARIABLES Victimes sexe et descend (MNAR) -> Absence liee a la variables elle meme
# # ============================================================

# In[ ]:


#TRAITEMENT DES VARIABLES Victime sexe et descend ,  (MNAR)
#Ces champs sont souvent manquants car la victime ne souhaite pas divulguer ou l’agent n’a pas pu obtenir l’info.
#Ainsi, l'Absence est liée à des facteurs sensibles, pas aléatoire.
#l'Action choisie est d'ajouter catégorie « Unknown ». Pour éviter de supprimer ces lignes et conserver l’info que « sexe/origine non déclaré ».

# nettoyage de Vict Sex : remplacer NA par Unknown
if "Vict Sex" in df.columns:
    # Étape 1 : Remplacer les valeurs incohérentes par NaN
    df["Vict Sex"] = df["Vict Sex"].replace(["X", "H","-", ""], np.nan)
    # Étape 2 : Remplacer les NaN par "Unknown"
    df["Vict Sex"] = df["Vict Sex"].fillna("Unknown")

# nettoyage de Vict Descent : remplacer NA par Unknown
if "Vict Descent" in df.columns:
    df["Vict Descent"] = df["Vict Descent"].fillna("Unknown")

df


# # ============================================================
# #  TRAITEMENT DE LA VARIABLE Victim age (MCAR)
# # ============================================================

# In[ ]:


#TRAITEMENT DE LA VARIABLE Victim age 
#Remplacer les valeurs invalides par NaN (valeur manquante logique) puis les imputer par la mediane des ages,
# --- Nettoyage des âges incohérents ---
import numpy as np

if "Vict Age" in df.columns:
    # 1️ Conversion en numérique et nettoyage des valeurs aberrantes - remplacer les valeurs aberantes (<0 ou >100) par NaN
    df["Vict Age"] = pd.to_numeric(df["Vict Age"], errors="coerce")
    df.loc[(df["Vict Age"] < 0) | (df["Vict Age"] > 100), "Vict Age"] = np.nan

    # 2️ Imputation par la médiane
    median_age = df["Vict Age"].median()
    df["Vict Age"] = df["Vict Age"].fillna(median_age)
    print(f"Age imputé : {median_age}")

    # 3️ Convertir en entier nullable (Int64)
    df["Vict Age"] = df["Vict Age"].astype("Int64")

    # 4️ Création des groupes d’âge à partir de Vict Age imputé
    df["Vict Age Group"] = pd.cut(
    df["Vict Age"],
    bins=[0, 12, 18, 30, 50, 70, 100],
    labels=["Child", "Teen", "Young Adult", "Adult", "Middle Age", "Senior"],
    include_lowest=True
)

    # 5️ Vérification finale
     # 8️ Afficher un résumé final
    print("\nRésumé après nettoyage :")
    print(df["Vict Age"].describe())
    print("Valeurs manquantes après imputation Vict Age :", df["Vict Age"].isna().sum())
    print("Valeurs manquantes après création Vict Age Group :", df["Vict Age Group"].isna().sum())



# # ============================================================
# # TRAITEMENT DES VARIABLES Lieu (Premis : MCAR)
# # ============================================================

# In[ ]:


#TRAITEMENT DES VARIABLES Lieu (Premis : MCAR/MAR)
#CETTE VARIABLE CONTIENT Très peu de manquants (<1%). Ici, ça ressemble à de la simple erreur de saisie.
#Ainsi, l'Action choisie sera de remplir par "NO DESC" Parce que le volume de manquants est insignifiant, pas besoin de méthode complexe

# Nettoyage Premis Desc : NA -> "NO DESC"
if "Premis Desc" in df.columns:
    df["Premis Desc"] = df["Premis Desc"].replace("-", np.nan)
    df["Premis Desc"] = df["Premis Desc"].fillna("NO DESC")

# Nettoyage Premis Cd : NA -> -1 (valeur speciale)
if "Premis Cd" in df.columns:
    df["Premis Cd"] = df["Premis Cd"].fillna(-1).astype(int)


# # ============================================================
# # TRAITEMENT DE LA VARIABLE Status (MCAR)
# # ============================================================

# In[ ]:


#TRAITEMENT DE LA VARIABLE Status (MCAR)
#Avec seulement 1 enregistrement manquant → typique d’un oubli isolé. l'Action choisie : remplacer par la valeur la plus fréquente.
#Pas d’impact sur l’analyse globale.
if "Status" in df.columns:
    mode_status = df["Status"].mode()[0]
    df["Status"] = df["Status"].fillna(mode_status)


# In[ ]:


print("Shape final:", df.shape)

# Vérifier si des NA restent
na_check = df.isna().sum().sort_values(ascending=False)
print(" Valeurs manquantes restantes :\n", na_check.head(50))


# # ============================================================
# # TRAITEMENT DES VARIABLES TEMPORELLES (DATES / HEURES)
# # ============================================================

# In[ ]:


#TRANSFORMATION DES VARIABLES DATE ET HEURES 
# ---  Transformation des dates et heures ---

# Conversion des deux colonnes de dates avec le bon format
date_format = "%m/%d/%Y %I:%M:%S %p"

df["DATE OCC"] = pd.to_datetime(df["DATE OCC"], format=date_format, errors="coerce")
df["Date Rptd"] = pd.to_datetime(df["Date Rptd"], format=date_format, errors="coerce")

# Vérifier que la conversion a bien fonctionné
print("Vérification de la conversion :")
print(df[["DATE OCC", "Date Rptd"]].head())

# Calcul du délai entre la survenue et le signalement (en jours)
df["Reporting Delay (days)"] = (df["Date Rptd"] - df["DATE OCC"]).dt.days

# Correction des anomalies : valeurs négatives = incohérences
# Si Date Rptd < DATE OCC → c’est une anomalie (erreur de saisie).
#On peut donc corriger ces cas en mettant le délai à NaN
df.loc[df["Reporting Delay (days)"] < 0, "Reporting Delay (days)"] = np.nan


#  Vérifications finales
print("\nRésumé statistique du délai :")
print(df["Reporting Delay (days)"].describe())


# Aperçu final
df[["DATE OCC", "Date Rptd", "Reporting Delay (days)"]].head(10)

