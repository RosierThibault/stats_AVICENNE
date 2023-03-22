import datetime

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import constantes as c

def statsServices(services=None, categorie=None):

    # initialise le dataframe
    df_ch = pd.DataFrame()

    # parcours le dictionnaire des CH pour concatener leur Dataframe dans un seul qui sera traiter
    for ch in c.LISTE_CH.values():
        df_ch = pd.concat([df_ch, ch])


    if categorie is not None:
        df_algo = pd.read_excel(c.FICHIER_TRAITE,sheet_name="Algorithmes", header=0,
                                names=['No_algo', 'Nom_Algorithme', 'Cat'], usecols="A:C")

        df_ch = pd.merge(df_ch, df_algo, on='No_algo')

        df_ch = df_ch[df_ch['Cat'].str.contains(categorie, na=False)]

    df_Service = pd.read_excel(c.FICHIER_TRAITE,sheet_name="Services", header=0,
                               names=['UF', 'CODE_UF', 'TYPE_UF'], usecols="A:C")

    df_ch = pd.merge(df_ch, df_Service, on='CODE_UF')

    stats_service = df_ch[['TYPE_UF']].drop_duplicates().reset_index(drop=True)

    if services is not None:
        df_ch = df_ch[df_ch['TYPE_UF'].isin(services)]

    alertes_algo = df_ch.groupby('TYPE_UF')["No_algo"].count()
    alertes_algo.name = 'Alertes'
    stats_service = pd.merge(stats_service, alertes_algo, on='TYPE_UF')

    plp_algo = df_ch.groupby('TYPE_UF')["PLP_existence"].apply(lambda x: x[x == "1 - Oui"].count())
    stats_service = pd.merge(stats_service, plp_algo, on='TYPE_UF')

    fauxp_ch = df_ch.groupby('TYPE_UF')["PLP_faux+"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, fauxp_ch, on='TYPE_UF')

    sans_plp = df_ch.groupby('TYPE_UF')["PLP_inexistant"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, sans_plp, on='TYPE_UF')

    plp_resolu = df_ch.groupby('TYPE_UF')["PLP_resolu"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, plp_resolu, on='TYPE_UF')

    plp_en_cours = df_ch.groupby('TYPE_UF')["PLP_en_cours_resolution"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, plp_en_cours, on='TYPE_UF')

    stats_service["Informatical PPV"] = (stats_service["Alertes"] - stats_service["PLP_faux+"]) / stats_service["Alertes"]

    stats_service["PDSS PPV"] = (stats_service["Alertes"] - stats_service["PLP_faux+"] - stats_service["PLP_inexistant"]) / \
                             stats_service["Alertes"]

    plp_cons = df_ch.groupby('TYPE_UF')["PLP_type_consequence"].apply(lambda x: x[x == "POTENTIELLE"].count())
    plp_cons.name = 'PLP - Conséquence Potentielle'
    stats_service = pd.merge(stats_service, plp_cons, on='TYPE_UF')

    plp_cons = df_ch.groupby('TYPE_UF')["PLP_type_consequence"].apply(lambda x: x[x == "AVEREE"].count())
    plp_cons.name = 'PLP - Conséquence Averee'
    stats_service = pd.merge(stats_service, plp_cons, on='TYPE_UF')

    ip_algo = df_ch.groupby('TYPE_UF')["IP_existence"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, ip_algo, on='TYPE_UF')

    trans_algo = df_ch.groupby('TYPE_UF')["IP_mode_trans"].apply(lambda x: x[x == "LAP"].count())
    trans_algo.name = 'LAP'
    stats_service = pd.merge(stats_service, trans_algo, on='TYPE_UF')

    trans_algo = df_ch.groupby('TYPE_UF')["IP_mode_trans"].apply(lambda x: x[x == "TEL"].count())
    trans_algo.name = 'TEL'
    stats_service = pd.merge(stats_service, trans_algo, on='TYPE_UF')

    trans_algo = df_ch.groupby('TYPE_UF')["IP_mode_trans"].apply(lambda x: x[x == "ORAL"].count())
    trans_algo.name = 'ORAL'
    stats_service = pd.merge(stats_service, trans_algo, on='TYPE_UF')

    ip_acc_algo = df_ch.groupby('TYPE_UF')["IP_acceptation"].apply(lambda x: x[x == "1 - Oui"].count())
    ip_acc_algo.name = 'IP Acceptée'
    stats_service = pd.merge(stats_service, ip_acc_algo, on='TYPE_UF')

    ip_acc_algo = df_ch.groupby('TYPE_UF')["IP_acceptation"].apply(
        lambda x: x[x == "2 - Oui, avec modification"].count())
    ip_acc_algo.name = 'IP Acceptée ac modif'
    stats_service = pd.merge(stats_service, ip_acc_algo, on='TYPE_UF')

    ip_acc_algo = df_ch.groupby('TYPE_UF')["IP_acceptation"].apply(lambda x: x[x == "0 - Non"].count())
    ip_acc_algo.name = 'IP refusée'
    stats_service = pd.merge(stats_service, ip_acc_algo, on='TYPE_UF')

    ip_acc_algo = df_ch.groupby('TYPE_UF')["IP_acceptation"].apply(lambda x: x[x == "3 - Non renseignée"].count())
    ip_acc_algo.name = 'IP Acc non renseigné'
    stats_service = pd.merge(stats_service, ip_acc_algo, on='TYPE_UF')

    stats_service["Acceptation PPV"] = (stats_service["IP Acceptée"] + stats_service["IP Acceptée ac modif"]) / stats_service[
        "IP_existence"]

    analyse_ante_algo = df_ch.groupby('TYPE_UF')["Analyse_ante"].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, analyse_ante_algo, on='TYPE_UF')

    plp_deja_detect_algo = df_ch.groupby('TYPE_UF')['PLP_deja_detecte'].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, plp_deja_detect_algo, on='TYPE_UF')

    plp_ip_add_algo = df_ch.groupby('TYPE_UF')['IP_additionnelle'].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, plp_ip_add_algo, on='TYPE_UF')

    plp_non_detect_algo = df_ch.groupby('TYPE_UF')['PLP_non_detecte'].apply(lambda x: x[x == 1].count())
    stats_service = pd.merge(stats_service, plp_non_detect_algo, on='TYPE_UF')

    plp_non_detect_ante_algo = df_ch.loc[(df_ch.PLP_non_detecte == 1) & (df_ch.Analyse_ante == 1)]
    plp_non_detect_ante_algo = plp_non_detect_ante_algo.groupby('TYPE_UF')["TYPE_UF"].count()
    plp_non_detect_ante_algo.name = 'PLP_non_detect_ante'
    stats_service = pd.merge(stats_service, plp_non_detect_ante_algo, how='left', on='TYPE_UF')

    age_moy = df_ch.assign(age=df_ch[['TYPE_UF', 'DDN', 'Date_analyse']].apply(age, axis=1))
    age_moy_grp = age_moy.groupby('TYPE_UF')['age'].mean()
    age_moy_grp.name = 'age moyen patient'
    stats_service = pd.merge(stats_service, age_moy_grp, on='TYPE_UF')


    col_list = list(stats_service)
    col_list.remove('TYPE_UF')
    col_list.remove('Informatical PPV')
    col_list.remove('PDSS PPV')
    col_list.remove('Acceptation PPV')
    col_list.remove('age moyen patient')

    total = stats_service[col_list].sum(numeric_only=True)
    stats_service = stats_service.append(total, ignore_index=True)


    stats_service.loc[stats_service.index[-1], 'Informatical PPV'] = ((stats_service.loc[stats_service.index[-1], 'Alertes']
                                                    -stats_service.loc[stats_service.index[-1], 'PLP_faux+'])
                                                    /stats_service.loc[stats_service.index[-1], 'Alertes'])



    stats_service.loc[stats_service.index[-1], 'PDSS PPV'] = ((stats_service.loc[stats_service.index[-1], 'Alertes']
                                                    -stats_service.loc[stats_service.index[-1], 'PLP_faux+']
                                                    -stats_service.loc[stats_service.index[-1], 'PLP_inexistant'])
                                                    /stats_service.loc[stats_service.index[-1], 'Alertes'])

    stats_service.loc[stats_service.index[-1], 'Acceptation PPV'] = ((stats_service.loc[stats_service.index[-1], 'IP Acceptée']
                                                    +stats_service.loc[stats_service.index[-1], 'IP Acceptée ac modif'])
                                                    /stats_service.loc[stats_service.index[-1], 'IP_existence'])


    stats_service.loc[stats_service.index[-1], 'age moyen patient'] = age_moy['age'].mean()

    stats_service.loc[stats_service.index[-1], 'TYPE_UF'] = 'TOTAL'

    stats_service['Informatical PPV'] = round(stats_service['Informatical PPV'], 2)
    stats_service['PDSS PPV'] = round(stats_service['PDSS PPV'], 2)
    stats_service['Acceptation PPV'] = round(stats_service['Acceptation PPV'], 2)
    stats_service['age moyen patient'] = round(stats_service['age moyen patient'], 2)


    stats_service[col_list] = stats_service[col_list].astype('Int64')

    return stats_service

def histo_services(categorie=None, services=None):

    # initialise le dataframe
    df_ch = pd.DataFrame()

    # parcours le dictionnaire des CH pour concatener leur Dataframe dans un seul qui sera traiter
    for ch in c.LISTE_CH.values():
        df_ch = pd.concat([df_ch, ch])

    df_service = pd.read_excel(c.FICHIER_TRAITE, sheet_name="Services", header=0,
                               names=['UF', 'CODE_UF', 'TYPE_UF'], usecols="A:C")

    df_ch = pd.merge(df_ch, df_service, on='CODE_UF')


    stats_service = df_ch[['TYPE_UF']].drop_duplicates().reset_index(drop=True)
    alertes_service = df_ch['TYPE_UF'].value_counts()
    alertes_service.name = "Alertes"
    PLP_existence = df_ch.groupby('TYPE_UF')["PLP_existence"].apply(lambda x: x[x == "1 - Oui"].count())
    PLP_resolu = df_ch.groupby('TYPE_UF')["PLP_resolu"].apply(lambda x: x[x == 1].count())
    PLP_en_cours = df_ch.groupby('TYPE_UF')["PLP_en_cours_resolution"].apply(lambda x: x[x == 1].count())

    IP_service = df_ch.groupby('TYPE_UF')["IP_existence"].apply(lambda x: x[x == 1].count())

    PLP_service = PLP_existence.add(PLP_resolu)
    PLP_service = PLP_service.add(PLP_en_cours)

    PLP_service.name = "PLP"
    IP_service.name = "IP"

    df = pd.concat([alertes_service, PLP_service, IP_service], axis=1)

    # stats_service = pd.merge(alertes_service, PLP_service, on='TYPE_UF')
    # stats_service = pd.merge(stats_service, IP_service, on='TYPE_UF')

    df.loc['EHPAD & LONG SEJOUR'] = df.loc[['EHPAD', 'GERIATRIE LONG']].sum()

    df = df.drop('EHPAD')
    df = df.drop('GERIATRIE LONG')
    df = df.sort_values(by="Alertes", ascending=False)

    ################ BROKEN Axis ################
    # fig, (ax1,ax2) = plt.subplots(2, 1, sharex=True, gridspec_kw={'height_ratios': [1,4]})
    # fig.subplots_adjust(hspace=0.01)
    #
    # ax1.set_ylim(5000, 5200)  # outliers only
    # ax1.spines['bottom'].set_visible(False)
    # ax1.xaxis.tick_top()
    #
    # ax2.set_ylim(0, 2500)
    # ax2.spines['top'].set_visible(False)
    #
    # d=0.01
    # kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
    # ax1.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
    # ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)
    # df.plot(ax=ax1, kind='bar', legend=True)
    #
    # kwargs = dict(transform=ax2.transAxes, color='k', clip_on=False)
    # ax2.plot((-d, +d), (1-d, 1+d), **kwargs)        # top-left diagonal
    # ax2.plot((1 - d, 1 + d), (1-d, 1+d), **kwargs)
    # df.plot(ax=ax2, kind='bar', legend=False, xlabel="Services", ylabel="NB")

    ################ BROKEN Axis END ################

    df.plot(kind='bar', legend=False, xlabel="Services", ylabel="NB")

    plt.tight_layout()

    plt.show()

def service_vs(liste_service1, liste_service2, categorie=None):

    df_service1 = statsServices(liste_service1, categorie)
    df_service1 = df_service1.tail(1)
    df_service1 = df_service1.reset_index(drop=True)
    df_service1.loc[0, 'TYPE_UF'] = ','.join(liste_service1)

    df_service2 = statsServices(liste_service2, categorie)
    df_service2 = df_service2.tail(1)
    df_service2 = df_service2.reset_index(drop=True)
    df_service2.loc[0, 'TYPE_UF'] = ','.join(liste_service2)

    return pd.concat([df_service1, df_service2], axis=0)


def age(row):
    ddn = row['DDN']
    dda = row['Date_analyse']

    try:
        return dda.year - ddn.year - ((dda.month, dda.day) < (ddn.month, ddn.day))

    except Exception:
        return pd.NA