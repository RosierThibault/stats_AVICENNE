import pandas as pd
import constantes as c
import scipy


# Renvoie un Dataframe avec les statistiques par algo
# categorie : nom de la catégorie voulu - si vide on garde tout
# categorieDel : nom de la catégorie à supprimer - si vide on garde tout
# liste_algo : liste des algos pour lesquels on récupère des stats - si vide tous les algos sont concernés
def statsEtab(categorie=None, categorieDel=None, liste_algo=None):

    # initialisation des variables
    alertesTotal = fauxpTotal = sans_plp_total = ipTotal = ip_acc_modif_total = ip_acc_total = tpsTotal = ageTotal = nbAge = nbTps = i = 0

    # initialisation des tableaux qui serviront au calcul d'indépendance du chi2
    # Tableau de x ligne et 2 colonnes (x = nb hopitaux)
    data_tel = [[0] * len(c.LISTE_CH) for _ in range(2)]
    data_aip = [[0] * len(c.LISTE_CH) for _ in range(2)]


    tabColName = ['Alertes', 'PLP', 'Faux positif', 'PLP non ex selon pharmacien', 'PLP resolu',
                'PLP en cours', 'Informatical PPV', 'PDSS PPV', 'PLP conseq Averee', 'PLP conseq Pot',
                'PLP risque conseq ', 'IP', 'Transmission LAP', 'Transmission TEL', 'Transmission ORAL',
                'IP Acceptee', 'IP Acc modif', 'IP refusee', 'IP Acc non renseignee', 'Acceptation PPV',
                'IP avec Analyse antérieur', 'IP deja detecte', 'PLP avec IP Additionnel',
                'IP non detecté par pharmacien', 'PLP non detecte malgres analyse ante',
                'tps analyse moyens alerte', 'age moyen patient', 'p-value Trans tel', 'p-value Acc IP']

    # initialisation du dataframe de resultat avec tabColName pour colonne
    df_etab = pd.DataFrame(columns=tabColName)

    # Si categorie non vide on charge le dataframe des algorithmes
    # Selection des algorithmes si ils appartienne à une catégorie
    if categorie is not None or categorieDel is not None:
        df_algo = pd.read_excel(c.FICHIER_TRAITE, sheet_name="Algorithmes", header=0,
                                names=['No_algo', 'Nom_Algorithme', 'Cat'], usecols="A:C")

    # pour chaque CH dans le dictionaire c.LISTE_CH on recuperere ses statistiques
    for ch_name, ch in c.LISTE_CH.items():

        # Si categorie ou categorieDel non vide on filtre
        if categorie is not None or categorieDel is not None:
            ch = pd.merge(ch, df_algo, on='No_algo')

            if categorie is not None:
                ch = ch[ch['Cat'].str.contains(categorie, na=False)]

            if categorieDel is not None:
                ch = ch[~ch['Cat'].str.contains(categorieDel, na=False)]

        # Si liste_algo non vide on filtre
        if liste_algo is not None:
            ch = ch[(ch['No_algo'].isin(liste_algo))]

        # Création des variables
        alertes_ch = ch['Date_analyse'].count()
        plp_ch = ch[ch["PLP_existence"] == '1 - Oui']["PLP_existence"].count()
        fauxp_ch = ch[ch["PLP_faux+"] == 1]["PLP_faux+"].count()
        sans_plp_ch = ch[ch["PLP_inexistant"] == 1]["PLP_inexistant"].count()
        plp_resolu_ch = ch[ch["PLP_resolu"] == 1]["PLP_resolu"].count()
        plp_en_cours_ch = ch[ch["PLP_en_cours_resolution"] == 1]["PLP_en_cours_resolution"].count()
        infoppv_ch = (alertes_ch - fauxp_ch) / alertes_ch
        pdssppv_ch = (alertes_ch - fauxp_ch - sans_plp_ch) / alertes_ch
        plp_cons_av_ch = ch[ch["PLP_type_consequence"] == "AVEREE"]["PLP_type_consequence"].count()
        plp_cons_pot_ch = ch[ch["PLP_type_consequence"] == "POTENTIELLE"]["PLP_type_consequence"].count()
        plp_cons_ris_ch = ch[ch["PLP_type_consequence"] == "Risque"]["PLP_type_consequence"].count()
        ip_ch = ch[ch["IP_existence"] == 1]["IP_existence"].count()
        trans_lap_ch = ch[ch["IP_mode_trans"] == 'LAP']["IP_mode_trans"].count()
        trans_tel_ch = ch[ch["IP_mode_trans"] == 'TEL']["IP_mode_trans"].count()
        trans_oral_ch = ch[ch["IP_mode_trans"] == 'ORAL']["IP_mode_trans"].count()
        ip_acc_ch = ch[ch["IP_acceptation"] == '1 - Oui']["IP_acceptation"].count()
        ip_acc_modif_ch = ch[ch["IP_acceptation"] == '2 - Oui, avec modification']["IP_acceptation"].count()
        ip_refus_ch = ch[ch["IP_acceptation"] == '0 - Non']["IP_acceptation"].count()
        acceptation_ppv_ch = (ip_acc_ch + ip_acc_modif_ch) / ip_ch
        ip_non_rens_ch = ch[ch["IP_acceptation"] == '3 - Non renseignée']["IP_acceptation"].count()
        analyse_ante_ch = ch[ch["Analyse_ante"] == 1]["Analyse_ante"].count()
        deja_detecte_ch = ch[ch["PLP_deja_detecte"] == 1]["PLP_deja_detecte"].count()
        ip_add_ch = ch[ch["IP_additionnelle"] == 1]["IP_additionnelle"].count()
        ip_non_detecte_ch = ch[ch["PLP_non_detecte"] == 1]["PLP_non_detecte"].count()
        ip_non_det_ana_ante_ch = ch[(ch["PLP_non_detecte"] == 1) & (ch["Analyse_ante"] == 1)]["Date_analyse"].count()
        tps_moy_ch = pd.to_numeric(ch["tps_analyse"], errors='coerce').mean()
        col_age = ch.assign(age=ch[['No_algo', 'DDN', 'Date_analyse']].apply(age, axis=1))
        age_moy_ch = col_age["age"].mean()

        # mise en Dataframe des variables de l'etablissement
        ligne_ch = [(alertes_ch, plp_ch, fauxp_ch, sans_plp_ch, plp_resolu_ch, plp_en_cours_ch, infoppv_ch, pdssppv_ch,
                    plp_cons_av_ch, plp_cons_pot_ch, plp_cons_ris_ch, ip_ch, trans_lap_ch, trans_tel_ch, trans_oral_ch,
                    ip_acc_ch, ip_acc_modif_ch, ip_refus_ch, ip_non_rens_ch,acceptation_ppv_ch, analyse_ante_ch,
                    deja_detecte_ch, ip_add_ch, ip_non_detecte_ch, ip_non_det_ana_ante_ch, tps_moy_ch, age_moy_ch,
                    '','')]

        df_ligne_ch = pd.DataFrame(ligne_ch, index=[ch_name], columns=tabColName)

        # Ajout de l'etablissement au resultat
        df_etab = pd.concat([df_etab, df_ligne_ch])

        # Mise a jour des colonnes calculé pour le total
        alertesTotal += alertes_ch
        fauxpTotal += fauxp_ch
        sans_plp_total += sans_plp_ch
        ipTotal += ip_ch
        ip_acc_modif_total += ip_acc_modif_ch
        ip_acc_total += ip_acc_ch
        tpsTotal += pd.to_numeric(ch["tps_analyse"], errors='coerce').sum()
        nbTps += ch["tps_analyse"].count()
        ageTotal += col_age["age"].sum()
        nbAge += col_age["age"].count()

        # Ajout des données de l'etablissement necessaire au calcul d'indépendance du chi2
        data_tel[i][0] = trans_tel_ch
        data_tel[i][1] = ip_ch - trans_tel_ch
        data_aip[i][0] = ip_acc_ch
        data_aip[i][1] = ip_ch - ip_acc_ch
        i += 1

    # recuperation des colonnes du TOTAL
    # on enlève celles qui ne sont pas sommable
    # on somme chaque colonne de cette liste
    # et on ajoute la ligne TOTAL au resultat
    col_list = list(df_etab)
    col_list.remove('Informatical PPV')
    col_list.remove('PDSS PPV')
    col_list.remove('Acceptation PPV')
    col_list.remove('tps analyse moyens alerte')
    col_list.remove('age moyen patient')
    total = df_etab[col_list].sum()
    total.name = "TOTAL"
    df_etab = df_etab.append(total)

    # calcul des p-value
    stat_tel, pv_tel, dof_tel, expected_tel = scipy.stats.chi2_contingency(data_tel, correction=False)
    stat_aip, pv_aip, dof_aip, expected_aip = scipy.stats.chi2_contingency(data_aip, correction=False)

    # Maj des cellules non sommable du TOTAL
    df_etab.loc['TOTAL', 'Informatical PPV'] = (alertesTotal - fauxpTotal) / alertesTotal
    df_etab.loc['TOTAL', 'PDSS PPV'] = (alertesTotal - fauxpTotal - sans_plp_total) / alertesTotal
    df_etab.loc['TOTAL', 'Acceptation PPV'] = (ip_acc_total + ip_acc_modif_total) / ipTotal
    df_etab.loc['TOTAL', 'tps analyse moyens alerte'] = tpsTotal/nbTps
    df_etab.loc['TOTAL', 'age moyen patient'] = ageTotal/nbAge
    df_etab.loc['TOTAL', 'p-value Trans tel'] = pv_tel
    df_etab.loc['TOTAL', 'p-value Acc IP'] = pv_aip
    df_etab.loc['Description p-value', 'p-value Trans tel'] = significatif(pv_tel)
    df_etab.loc['Description p-value', 'p-value Acc IP'] = significatif(pv_aip)

    return df_etab

def age(row):
    ddn = row['DDN']
    dda = row['Date_analyse']

    try:
        return dda.year - ddn.year - ((dda.month, dda.day) < (ddn.month, ddn.day))

    except Exception:
        return pd.NA

def significatif(pv):
    if pv < 0.01:
        return "Difference statistiquement significative (p-value < 0.01)"
    elif 0.05 < pv <= 0.01:
        return "Difference statistiquement significative (p-value < 0.05)"
    else:
        return "Difference statistiquement non significative (p-value > 0.05)"