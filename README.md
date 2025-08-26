# Test Technique – Traitement de Données Énergétiques avec PySpark

## 📁 Fichiers fournis

- `inverter_yields.csv` : Mesures toutes les 10 minutes par onduleur
- `static_inverter_info.csv` : Métadonnées des onduleurs
- `sldc_events.csv` : Événements sur les équipements (IEC category)
- `site_median_reference.csv` : Valeurs de rendement spécifiques de référence par site

## 🎯 Objectif

Construire un pipeline PySpark local qui :

1. Lit les 4 jeux de données CSV
2. Joints `inverter_yields` avec :
   - `static_inverter_info` via `logical_device_mrid`
   - `sldc_events` en chevauchement temporel
   - `site_median_reference` sur `project_code` et `ts_start`
3. Calcule `potential_production = specific_yield_ac × ac_max_power × 1/6` (10min en heures)
4. Ne conserve que les inverters `"PV"` qui **ne sont pas "AC-Coupled"**
5. Produit un fichier `parquet` partitionné par `project_code` et `year_month`

## 📊 Bonus

- Écrire une requête SQL pour les sites sous-performants
- Proposer un schéma Glue/Athena adapté
- Ajouter des tests Spark
- Faire en sorte que la Pipeline tourne sur n'importe quel environnement.

## 🚚 Delivery

Créer un **fork** de ce projet (**pas de nouvelle branche**) et rajoutez les users [jaumes5](https://github.com/jaumes5) et 
[DieuveilleWebH3](https://github.com/DieuveilleWebH3).

Bon courage !
