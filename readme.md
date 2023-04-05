# STAGE ADL PARTNER

Stage de 2 mois effectué au sein de la société ADL Partner.
Le langage utilisé est le PL/SQL.


## Projet: réecriture d'un traitement

__Fichiers concernées:__
* charge_avis_pkg
	* en-tête du package déclarant procédure et fonctions
* charge_avis_pkg_body
	* corps du package où se trouvent les corps des procédures et fonctions

L'entreprise *ADL Partner* possède une branche chargé de vendre des abonnements pour les éditeurs de magazine.
Lorsqu'il y a un changement d'adresse, une suspension, une suppression ou une création d'abonnement, il faut structurer les informations relatives à ces évènements avant d'en faire part à l'éditeur, ces "comptes-rendu", se nomment des avis.

Mon travail était de recréer le processus de traitement des avis, crée à l'origine à l'aide d'un logiciel nommé *GENIO*, sous la forme d'un *Packge PL/SQL* afin de rendre le processus maintenable par le personnel ne maîtrisant pas *GENIO* ainsi que d'éliminer la création de fichier intermédiaire, vestige de l'ancienne configuration.

Le traitement extrait des informations de deux tables de deux bases de données différente puis traite ces informations avant de les placer dans une table de sortie où d'autre processus pourront fabriquer les avis sous forme de document.

Le package possède une procédure correspondant à un module *GENIO* de l'ancien processus. La procédure "Charge-avis" permettant d'éxécuter l'ensemble des procédures du package dans l'ordre.

### Schéma de fonctionnement:

* Extraction des données venant de tables et de vues diverses
* Mise à jour des informations relatives aux adresses et au produits dans la table tampon
* Test
	* vérification de la cohérence des informations entre la table tampon et les informations présentes en production
* Basculement de toute les informations vers la table de sortie


## Travail suivant:

__Fichier concerné:__
* creation_avis

Permet de commencer le processus de fabrication des avis.

* Extraction des informations de la table de sortie
* Appel d'une procédure PL/SQL de création des fichiers sur le serveur

