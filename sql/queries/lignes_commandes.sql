SELECT
id_eteko AS id_commande,
id_lig AS id,
id_produit,
produit,
section,
designation_personnalisee,
description,
quantite,
prix_u_ht,
taux_tva,
total_ht,
total_ttc,
remise_en_ AS type_remise,
total_ttc_remise,
total_ht_remise,
total_tva_remise
FROM BI_commande_lignes_de_commande