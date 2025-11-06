## Instructions pour l'agent Copilot

But
----
Ce dépôt contient un utilitaire shell dont l'objectif est d'extraire des données MySQL au format CSV puis d'envoyer le(s) fichier(s) résultant(s) sur un serveur SFTP. Ce fichier décrit précisément les contraintes, l'API (variables d'environnement et arguments), les critères d'acceptation et des exemples d'exécution pour guider la génération automatique du script `scripts/mysql_to_sftp.sh`.

Contrainte principale
--------------------
- Implémentation en shell (bash). Ne pas utiliser Python/Node pour le coeur du script.
- Compatible POSIX/bash moderne : utiliser `#!/usr/bin/env bash` et `set -euo pipefail`.
- Favoriser l'authentification par clef SSH pour SFTP. Le mot de passe SFTP peut être pris en charge via `sshpass` si nécessaire (documenter ce risque).
- Doit fonctionner sur une image Linux standard (Debian/Ubuntu) avec les paquets usuels (`mysql-client` ou `mariadb-client`, `openssh-client`, `gzip`, `sshpass` optionnel).

Emplacement et nom du script
----------------------------
- Chemin attendu : `scripts/mysql_to_sftp.sh` (exécutable, mode 755).

Contract (Inputs / Outputs / Erreurs)
-----------------------------------
- Inputs : variables d'environnement (voir section suivante) et/ou arguments CLI minimal (`--query` ou `--table`).
- Outputs : fichier CSV (optionnellement compressé en .gz) placé localement puis téléversé sur le serveur SFTP.
- Codes de sortie : 0 = succès ; >0 = échec. Messages d'erreur significatifs doivent être loggés vers stderr.

Variables d'environnement attendues
----------------------------------
- DB_HOST (ex: db.example.com)
- DB_PORT (ex: 3306)
- DB_USER
- DB_PASS
- DB_NAME
- DB_QUERY (optionnel si `--table` fourni)
- DB_TABLE (optionnel)
- OUTPUT_DIR (répertoire local pour écrire le CSV, ex: /tmp/exports)
- FILENAME_TEMPLATE (ex: "export_%Y%m%d_%H%M%S.csv") — utilisé avec `date +"..."` pour générer le nom
- COMPRESS (true|false) — si true, gzip le CSV après création
- SFTP_HOST
- SFTP_PORT (par défaut 22)
- SFTP_USER
- SFTP_PASS (optionnel, encourager clef privée)
- SFTP_KEY_PATH (chemin vers la clef privée SSH, optionnel)
- SFTP_REMOTE_DIR (répertoire distant où déposer le fichier)
- RETRIES (nombre de tentatives d'upload, ex: 3)
- RETRY_DELAY (secondes entre essais)
- LOG_LEVEL (INFO|DEBUG|ERROR)

Options CLI recommandées
------------------------
- --query "SQL"       : exécuter la requête SQL fournie
- --table TABLE_NAME   : exporter toute la table (équivalent à `SELECT * FROM TABLE`)
- --out /path/to/file  : chemin du fichier de sortie (surcharge `OUTPUT_DIR` + template)
- --dry-run            : n'écrit pas sur le disque ni n'upload, affiche les étapes
- --verbose            : active LOG_LEVEL=DEBUG

Comportement attendu
---------------------
1. Charger un fichier `.env` si présent (ex: `configs/example.env`) sans exposer les valeurs dans les logs. Recommander `set -a; source .env; set +a` pour exporter.
2. Valider les variables requises. Si manquantes, quitter avec un message utile.
3. Construire la commande MySQL pour exporter en CSV. Exemple :
   - Utiliser le client `mysql` en mode batch : `mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --batch --raw -e "$SQL" > "$OUTFILE"`
   - S'assurer que les séparateurs et les en-têtes conviennent (exposer une option pour inclure/exclure les en-têtes).
4. Optionnel : compresser le fichier en `.gz` si `COMPRESS=true`.
5. Transférer vers le serveur SFTP :
   - Si `SFTP_KEY_PATH` fourni : utiliser `sftp -i "$SFTP_KEY_PATH" -P "$SFTP_PORT" "$SFTP_USER"@"$SFTP_HOST"` avec un batchfile `put "$LOCAL_FILE" "$SFTP_REMOTE_DIR/"`.
   - Sinon si `SFTP_PASS` fourni et `sshpass` installé : `sshpass -p "$SFTP_PASS" sftp -oBatchMode=no -P "$SFTP_PORT" "$SFTP_USER"@"$SFTP_HOST"`.
6. Implémenter un mécanisme de retries configurable pour l'upload.
7. Nettoyer les fichiers temporaires dans un trap EXIT. Ne pas supprimer l'archive finale en cas de succès (option `--keep-local` possible).

Exemples d'utilisation
------------------------
1) Exécution simple (clé SSH) :

   export DB_HOST=db.example.com DB_USER=reporter DB_PASS=secret DB_NAME=shop
   export SFTP_HOST=sftp.example.com SFTP_USER=upload SFTP_KEY_PATH=/home/user/.ssh/id_rsa SFTP_REMOTE_DIR=/incoming
   ./scripts/mysql_to_sftp.sh --table orders

2) Via un fichier `.env` (configs/example.env) :

   set -a; source configs/example.env; set +a
   ./scripts/mysql_to_sftp.sh --query "SELECT id, total, created_at FROM orders WHERE created_at >= '2025-01-01'"

3) Cron (exemple) :

   0 2 * * * cd /workspace/mysql_to_ftp && set -a; source configs/example.env; set +a && ./scripts/mysql_to_sftp.sh --table orders --compress

Critères d'acceptation (tests manuels / automatisables)
-------------------------------------------------------
- Le script `scripts/mysql_to_sftp.sh` existe et est exécutable.
- Avec des variables valides et une base accessible, le script écrit un fichier CSV localement puis l'upload sur le répertoire SFTP ciblé.
- Le script renvoie 0 en cas de succès et >0 en cas d'échec, avec messages d'erreur clairs sur stderr.
- Option `--dry-run` affiche les actions sans exécution.
- Les tentatives d'upload échouées respectent la valeur `RETRIES` et attendent `RETRY_DELAY` entre essais.

Tests suggérés
--------------
- Tests unitaires shell : utiliser `bats-core` pour créer des tests qui simulent l'environnement (exporter variables), mocker `mysql` et `sftp` via petites fonctions PATH shim.
- Tests d'intégration : sur une machine d'essai, lancer MySQL local et un serveur SFTP (ex: `sshd` avec subsystem sftp) et vérifier upload réel.

Sécurité
--------
- Ne pas committer de secrets. Fournir `configs/example.env` avec valeurs factices uniquement.
- Préférer les clés SSH. Si `SFTP_PASS` est utilisé, documenter l'usage de `sshpass` et ses risques.

Livrables attendus
------------------
- `scripts/mysql_to_sftp.sh` : script complet et testé de bout en bout.
- `configs/example.env` : fichier d'exemple listant toutes les variables.
- `README.md` : section « Usage » avec exemples montrés plus haut.
- `tests/` : dossier avec tests `bats` ou scripts de test.

Remarques pour l'implémentation
-------------------------------
- Respecter des fonctions claires : `load_config`, `validate`, `export_csv`, `compress_file`, `upload_sftp`, `cleanup`, `log`.
- Journaux : envoyer INFO/ERROR sur stdout/stderr, permettre `--verbose` pour DEBUG.
- Éviter d'exposer `DB_PASS` et `SFTP_PASS` dans les logs.

Si vous voulez, je peux :
- Générer immédiatement le script `scripts/mysql_to_sftp.sh` basé sur ces instructions.
- Ajouter `configs/example.env` et un README d'exécution.

Fin du fichier d'instructions.
