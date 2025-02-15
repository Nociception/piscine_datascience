#!/bin/bash

declare -A HELP_MESSAGES

HELP_MESSAGES["help"]="Affiche cette aide. Utilisation : make help [règle]"
HELP_MESSAGES["up"]="Démarre les conteneurs et initialise la base de données."
HELP_MESSAGES["down"]="Arrête et supprime les conteneurs sans toucher aux volumes."
HELP_MESSAGES["rm_volumes"]="Supprime tous les volumes Docker pour réinitialiser les données."
HELP_MESSAGES["psql"]="Ouvre une session PostgreSQL en ligne de commande."
HELP_MESSAGES["ex01"]="Exécute le script Python associé à l'exercice 01."
HELP_MESSAGES["rebuild_image"]="Reconstruit une image Docker. Utilisation : make rebuild_image SERVICE=<nom_du_service>"

if [ -n "$1" ]; then
    # Si un argument est fourni, afficher l’aide spécifique
    if [[ -v HELP_MESSAGES["$1"] ]]; then
        echo -e "\033[1;34m$1\033[0m - ${HELP_MESSAGES[$1]}"
    else
        echo -e "\033[1;31mErreur : règle inconnue '$1'\033[0m"
        echo "Utilisez 'make help' pour voir les règles disponibles."
    fi
else
    # Sinon, afficher l’aide générale
    echo -e "\033[1;33mListe des commandes disponibles :\033[0m"
    for rule in "${!HELP_MESSAGES[@]}"; do
        echo -e "\033[1;34m$rule\033[0m - ${HELP_MESSAGES[$rule]}"
    done
fi