import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# Dossier d'exportation des resultats
# Fichier excel des données AVICENNE
DOSSIER_EXPORT = 'C:/Users/Thibault/Documents/CHL/AVICENNE/resultats/'
FICHIER_TRAITE = 'C:/Users/Thibault/Documents/CHL/AVICENNE/AVICENNE - Base des analyses pharmaceutiques - AVICENNE remake.xlsx'

TABCOLNAME=['NOM_Prenom', 'DDN', 'NIP', 'CODE_UF', 'Date_analyse', 'No_algo', 'PLP_existence', 'PLP_faux+',
            'PLP_inexistant', 'PLP_resolu', 'PLP_en_cours_resolution', 'PLP_CODE_SFPC', 'PLP_type_consequence',
            'PLP_consequence', 'IP_existence', 'IP_CODE_SFPC', 'IP_descriptif', 'IP_mode_trans', 'IP_acceptation',
            'Analyse_ante', 'PLP_deja_detecte', 'PLP_non_detecte', 'IP_additionnelle', 'tps_analyse']

# Chargement des dataframes
DF_CHRU = pd.read_excel(FICHIER_TRAITE, sheet_name=0, header=3, names=TABCOLNAME, usecols="A:G,I:S,U,W:AA")
DF_CHL = pd.read_excel(FICHIER_TRAITE, sheet_name=1, header=3, names=TABCOLNAME, usecols="A:G,I:S,U,W:AA")

# Dictionaire des CH, permettant de choisir les hopitaux on analyse les données
LISTE_CH = {'CHRU': DF_CHRU, 'CHL': DF_CHL}