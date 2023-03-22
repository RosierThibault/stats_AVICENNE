import pandas as pd
import constantes as c


# Renvoie un Dataframe avec les statistiques par algo
# categorie : nom de la catégorie voulu - si vide on garde tout
# liste_algo : liste des algos pour lequels on récupère des stats - si vide tous les algos sont concernés
def statsAlgo(categorie=None, liste_algo=None):
    df_algo = pd.read_excel(c.FICHIER_TRAITE, sheet_name="Algorithmes", header=0,
                            names=['No_algo', 'Nom_Algorithme', 'Cat1', 'Cat2'], usecols="A:D")
    # initialise le dataframe
    df_ch = pd.DataFrame()

    # parcours le dictionnaire des CH pour concatener leur Dataframe dans un seul qui sera traiter
    for ch in c.LISTE_CH.values():
        df_ch = pd.concat([df_ch, ch])

    df_ch = pd.merge(df_ch, df_algo, on='No_algo')

    stats_algo = df_ch[['No_algo']].drop_duplicates().reset_index(drop=True)

    # Si categorie non vide on filtre
    if categorie is not None:
        df_ch = df_ch[df_ch['Cat'].str.contains(categorie, na=False)]

    # Si liste_algo non vide on filtre
    if liste_algo is not None:
        df_ch = df_ch[(df_ch['No_algo'].isin(liste_algo))]

    # fusion du dataframe de stats et de la liste des algos pour obtenir plus d'information sur eux
    # la fusion se réalise sur le Numero d'algo qui est present dans les 2 dataframes
    stats_algo = pd.merge(stats_algo, df_algo, on='No_algo')

    # Récupération des different composant du dataframe de résultat
    # Les données sont groupées par numero d'algo pour pouvoir compter les données pour chaque algo
    # Si besoin on attribue un nom a la colonne
    # On ajoute notre colonne dans le dataframe de resultat
    alertes_algo = df_ch.groupby('No_algo')["No_algo"].count()
    alertes_algo.name = 'Alertes'
    stats_algo = pd.merge(stats_algo, alertes_algo, on='No_algo')

    # Pour chaque PLP_existence x si x = 1 - Oui on le comptabilise
    # Les autres ligne lambda fonctionnent de la meme manière
    plp_algo = df_ch.groupby('No_algo')["PLP_existence"].apply(lambda x: x[x == "1 - Oui"].count())
    stats_algo = pd.merge(stats_algo, plp_algo, on='No_algo')

    fauxp_ch = df_ch.groupby('No_algo')["PLP_faux+"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, fauxp_ch, on='No_algo')

    sans_plp = df_ch.groupby('No_algo')["PLP_inexistant"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, sans_plp, on='No_algo')

    plp_resolu = df_ch.groupby('No_algo')["PLP_resolu"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, plp_resolu, on='No_algo')

    plp_en_cours = df_ch.groupby('No_algo')["PLP_en_cours_resolution"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, plp_en_cours, on='No_algo')

    stats_algo["Informatical PPV"] = (stats_algo["Alertes"] - stats_algo["PLP_faux+"]) / stats_algo["Alertes"]

    stats_algo["PDSS PPV"] = (stats_algo["Alertes"] - stats_algo["PLP_faux+"] - stats_algo["PLP_inexistant"]) / \
                             stats_algo["Alertes"]

    plp_cons = df_ch.groupby('No_algo')["PLP_type_consequence"].apply(lambda x: x[x == "POTENTIELLE"].count())
    plp_cons.name = 'PLP - Conséquence Potentielle'
    stats_algo = pd.merge(stats_algo, plp_cons, on='No_algo')

    plp_cons = df_ch.groupby('No_algo')["PLP_type_consequence"].apply(lambda x: x[x == "AVEREE"].count())
    plp_cons.name = 'PLP - Conséquence Averee'
    stats_algo = pd.merge(stats_algo, plp_cons, on='No_algo')

    ip_algo = df_ch.groupby('No_algo')["IP_existence"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, ip_algo, on='No_algo')

    trans_algo = df_ch.groupby('No_algo')["IP_mode_trans"].apply(lambda x: x[x == "LAP"].count())
    trans_algo.name = 'LAP'
    stats_algo = pd.merge(stats_algo, trans_algo, on='No_algo')

    trans_algo_tel = df_ch.groupby('No_algo')["IP_mode_trans"].apply(lambda x: x[x == "TEL"].count())
    trans_algo_tel.name = 'TEL'
    stats_algo = pd.merge(stats_algo, trans_algo_tel, on='No_algo')

    trans_algo = df_ch.groupby('No_algo')["IP_mode_trans"].apply(lambda x: x[x == "ORAL"].count())
    trans_algo.name = 'ORAL'
    stats_algo = pd.merge(stats_algo, trans_algo, on='No_algo')

    ip_acc_algo = df_ch.groupby('No_algo')["IP_acceptation"].apply(lambda x: x[x == "1 - Oui"].count())
    ip_acc_algo.name = 'IP Acceptée'
    stats_algo = pd.merge(stats_algo, ip_acc_algo, on='No_algo')

    ip_acc_algo = df_ch.groupby('No_algo')["IP_acceptation"].apply(
        lambda x: x[x == "2 - Oui, avec modification"].count())
    ip_acc_algo.name = 'IP Acceptée ac modif'
    stats_algo = pd.merge(stats_algo, ip_acc_algo, on='No_algo')

    ip_acc_algo = df_ch.groupby('No_algo')["IP_acceptation"].apply(lambda x: x[x == "0 - Non"].count())
    ip_acc_algo.name = 'IP refusée'
    stats_algo = pd.merge(stats_algo, ip_acc_algo, on='No_algo')

    ip_acc_algo = df_ch.groupby('No_algo')["IP_acceptation"].apply(lambda x: x[x == "3 - Non renseignée"].count())
    ip_acc_algo.name = 'IP Acc non renseigné'
    stats_algo = pd.merge(stats_algo, ip_acc_algo, on='No_algo')

    stats_algo["Acceptation PPV"] = (stats_algo["IP Acceptée"] + stats_algo["IP Acceptée ac modif"]) / stats_algo[
        "IP_existence"]

    analyse_ante_algo = df_ch.groupby('No_algo')["Analyse_ante"].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, analyse_ante_algo, on='No_algo')

    plp_deja_detect_algo = df_ch.groupby('No_algo')['PLP_deja_detecte'].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, plp_deja_detect_algo, on='No_algo')

    plp_ip_add_algo = df_ch.groupby('No_algo')['IP_additionnelle'].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, plp_ip_add_algo, on='No_algo')

    plp_non_detect_algo = df_ch.groupby('No_algo')['PLP_non_detecte'].apply(lambda x: x[x == 1].count())
    stats_algo = pd.merge(stats_algo, plp_non_detect_algo, on='No_algo')

    plp_non_detect_ante_algo = df_ch.loc[(df_ch.PLP_non_detecte == 1) & (df_ch.Analyse_ante == 1)]
    plp_non_detect_ante_algo = plp_non_detect_ante_algo.groupby('No_algo')["No_algo"].count()
    plp_non_detect_ante_algo.name = 'PLP_non_detect_ante'
    stats_algo = pd.merge(stats_algo, plp_non_detect_ante_algo, how='left', on='No_algo')

    age_moy = df_ch.assign(age=df_ch[['No_algo', 'DDN', 'Date_analyse']].apply(age, axis=1))
    age_moy_grp = age_moy.groupby('No_algo')['age'].mean()
    age_moy_grp.name = 'age moyen patient'
    stats_algo = pd.merge(stats_algo, age_moy_grp, on='No_algo')

    # creation liste des colonnes
    # suppression des colonnes non additionnable
    # Somme des colonnes restante dans la liste
    col_list = list(stats_algo)
    col_list.remove('No_algo')
    col_list.remove('Nom_Algorithme')
    col_list.remove('Cat1')
    col_list.remove('Cat2')
    col_list.remove('Informatical PPV')
    col_list.remove('PDSS PPV')
    col_list.remove('Acceptation PPV')
    col_list.remove('age moyen patient')

    total = stats_algo[col_list].sum(numeric_only=True)
    total.name = "TOTAL"
    stats_algo = stats_algo.append(total)

    # Calculs des Colonnes numerique non additionnables
    stats_algo.loc['TOTAL', 'Informatical PPV'] = ((stats_algo.loc['TOTAL', 'Alertes']
                                                    - stats_algo.loc['TOTAL', 'PLP_faux+'])
                                                   / stats_algo.loc['TOTAL', 'Alertes'])

    stats_algo.loc['TOTAL', 'PDSS PPV'] = ((stats_algo.loc['TOTAL', 'Alertes']
                                            - stats_algo.loc['TOTAL', 'PLP_faux+']
                                            - stats_algo.loc['TOTAL', 'PLP_inexistant'])
                                           / stats_algo.loc['TOTAL', 'Alertes'])

    stats_algo.loc['TOTAL', 'Acceptation PPV'] = ((stats_algo.loc['TOTAL', 'IP Acceptée']
                                                   + stats_algo.loc['TOTAL', 'IP Acceptée ac modif'])
                                                  / stats_algo.loc['TOTAL', 'IP_existence'])

    stats_algo.loc['TOTAL', 'age moyen patient'] = age_moy['age'].mean()

    stats_algo.loc['TOTAL', 'No_algo'] = 0
    stats_algo.loc['TOTAL', 'Nom_Algorithme'] = 'TOTAL'

    # Arrondi des colonnesa 2 chiffres apres la virgule
    stats_algo['Informatical PPV'] = round(stats_algo['Informatical PPV'], 2)
    stats_algo['PDSS PPV'] = round(stats_algo['PDSS PPV'], 2)
    stats_algo['Acceptation PPV'] = round(stats_algo['Acceptation PPV'], 2)
    stats_algo['age moyen patient'] = round(stats_algo['age moyen patient'], 2)

    stats_algo = stats_algo.astype({'No_algo': int})

    # Int64 permet un affichage entier (2.0 -> 2)
    stats_algo[col_list] = stats_algo[col_list].astype('Int64')

    # suppression des colones Cat qui n'ont pas d'intérêt dans le dataframe résultat
    stats_algo = stats_algo.drop(['Cat1', 'Cat2'], axis=1)

    return stats_algo