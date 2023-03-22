import pandas as pd
import constantes as c
import statsAlgo
import statsEtab
import statsServices


# Statistiques par établissement
# df_etab = statsEtab.statsEtab(liste_algo=[269])
# df_etab.to_csv(c.DOSSIER_EXPORT + "stat_etab.csv", sep=";")

# # Statistiques pour les services dans le tableau tab_services
tab_services = ["MEDECINE", "CHIRURGIE", "USC", "REA", "GERIATRIE LONG", "AUTRES", "CONSULT", "EPHAD", "CARDIO"]
df_ga_autre = statsServices.service_vs(tab_services, ["GERIATRIE AIGUE"])
df_ga_autre.to_csv(c.DOSSIER_EXPORT + "stat_ga_services.csv", sep=";",index=False)

df_service = statsServices.statsServices()
df_service.to_csv(c.DOSSIER_EXPORT +"stat_services.csv", sep=";",index=False)