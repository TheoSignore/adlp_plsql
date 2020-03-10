create or replace package body                 charge_avis_pkg as
    /*
        Package dont les procédures constituent le process de traitement des avis éditeurs.
        Package écrit par un stagiaire.
    */
    log_file utl_file.file_type := UTL_FILE.FOPEN('/mnt/grfa/Systeme_outputdir/charge_avis/', 'charge_avis_log'||to_char(sysdate,'yyyy-mm-dd_hh-mi-ss')||'.txt', 'a');
    
    ftab constant char(1) := chr(9);
    dbtab constant char(2) := chr(9)||chr(9);
    rtrn constant char(1) := chr(13) ;
    dt_traitement date;
    
    procedure charge_avis is
        /*
            Cette procédure permet de lancer les autres procédures du package dans le bon ordre.
        */
        ecart EXCEPTION; 
    begin
        utl_file.put_line(log_file,to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> Chargement des avis:');
        begin p010_quantieme ; end;
        begin p020_trunc_event_client_tmp ; end;
        begin p030_sygal ; end;
        begin p040_adlm ; end;
        begin p050_maj_ec_tmp_add_adl ; end;
        begin p060_ann_an3_74 ; end;
        begin p070_code_statut0 ; end;
        begin p080_150_annulation_periode ; end;
        begin p090_reinst_periode; end;
        begin p100_rep_periode; end;
        begin p110_produit ; end;
        begin p120_type_event ; end;
        begin p130_date_planning ; end;
        begin p140_chrono_dest ; end;
        begin p080_150_annulation_periode ; end;
        begin p160_updchrono_dest ; end;
        begin p170_remise_status ; end;
        begin p180_generation_mes ; end;
        begin p190_offre ; end;
        begin
            /*
                Arrête à la procédure en cas d'écart entre la prod et la test
            */
            if  verif_compte = 1 then
                utl_file.put_line(log_file,'ECART AVEC LA PROD: ARRET DE LA PROCEDURE');
                utl_file.fclose(log_file);
                raise ecart;
            end if;
        end;
        begin p200_supp_code_statut0 ; end;
        begin p210_tmp2event_client ; end;
        begin p220_intercept_num ; end;
        begin p230_maj_quantieme ; end ;
        utl_file.put_line(log_file,to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        utl_file.fclose(log_file);
        exception
            WHEN ecart THEN null;
            when others then
                utl_file.put_line(log_file,'UNE ERREUR EST SURVENU: '||SQLERRM);
                utl_file.fclose(log_file);
                rollback;
    end charge_avis;
    
    function verif_compte RETURN number AS 
            /*
                fonction comparant le nombre d'avis selon les critères
                et écrit le tout dans un fichier de log
                
                s'il y a un écart, elle retourne 0, sinon 1
            */
            cntref int;
            cnttst int;
            diff int := 0;
            
            ecart number(1) := 0 ;
        BEGIN
            utl_file.put_line(log_file,dbtab||'Début des comptes:');
            utl_file.put_line(log_file,dbtab||' ');
        
            select count(*) into cnttst from systeme.evenements_clients_tmp;  
            select count(*) into cntref from systeme.evenements_clients_rec;
            diff := cnttst - cntref ;
            utl_file.put_line(log_file,dbtab||'TOTAL PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'TOTAL TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'TOTAL DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cnttst from systeme.evenements_clients_tmp where systeme_vente = 'SYG';
            select count(*) into cntref from systeme.evenements_clients_rec where systeme_vente = 'SYG';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'SYGAL PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'SYGAL TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'SYGAL DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cnttst from systeme.evenements_clients_tmp where systeme_vente = 'ADM';
            select count(*) into cntref from systeme.evenements_clients_rec where systeme_vente = 'ADM';
            diff := cnttst - cntref ;
            utl_file.put_line(log_file,dbtab||'ADLM PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'ADLM TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'ADLM DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            
            select count(*) into cnttst from systeme.evenements_clients_tmp where dateacquisition_fichier = dt_traitement;  
            select count(*) into cntref from systeme.evenements_clients_rec where dateacquisition_fichier = dt_traitement;
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'TOTAL PROD AJDH: '||cntref);
            utl_file.put_line(log_file,dbtab||'TOTAL TEST AJDH: '||cnttst);
            utl_file.put_line(log_file,dbtab||'TOTAL DIFF AJDH: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cnttst from systeme.evenements_clients_tmp where systeme_vente = 'SYG' and dateacquisition_fichier = dt_traitement;
            select count(*) into cntref from systeme.evenements_clients_rec where systeme_vente = 'SYG' and dateacquisition_fichier = dt_traitement; 
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'SYGAL PROD AJDH: '||cntref);
            utl_file.put_line(log_file,dbtab||'SYGAL TEST AJDH: '||cnttst);
            utl_file.put_line(log_file,dbtab||'SYGAL DIFF AJDH: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where systeme_vente = 'ADM' and dateacquisition_fichier = dt_traitement;
            select count(*) into cnttst from systeme.evenements_clients_tmp where systeme_vente = 'ADM' and dateacquisition_fichier = dt_traitement;
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'ADLM PROD AJDH: '||cntref);
            utl_file.put_line(log_file,dbtab||'ADLM TEST AJDH: '||cnttst);
            utl_file.put_line(log_file,dbtab||'ADLM DIFF AJDH: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'AN3';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'AN3';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'AN3 PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'AN3 TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'AN3 DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'ANN';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'ANN';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'ANN PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'ANN TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'ANN DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHA';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHA';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHA PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHA TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHA DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHB';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHB';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHB PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHB TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHB DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHD';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHD';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHD PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHD TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHD DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHF';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHF';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHF PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHF TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHF DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHL';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHL';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHL PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHL TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHL DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'CHP';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'CHP';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'CHP PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'CHP TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'CHP DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'EXT';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'EXT';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'EXT PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'EXT TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'EXT DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'INS';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'INS';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'INS PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'INS TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'INS DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'INT';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'INT';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'INT PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'INT TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'INT DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'MES';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'MES';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'MES PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'MES TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'MES DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'PRO';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'PRO';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'PRO PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'PRO TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'PRO DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'REI';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'REI';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'REI PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'REI TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'REI DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'REP';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'REP';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'REP PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'REP TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'REP DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'RES';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'RES';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'RES PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'RES TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'RES DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'REX';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'REX';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'REX PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'REX TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'REX DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            select count(*) into cntref from systeme.evenements_clients_rec where type_evenement = 'SUS';
            select count(*) into cnttst from systeme.evenements_clients_tmp where type_evenement = 'SUS';
            diff := cnttst - cntref;
            utl_file.put_line(log_file,dbtab||'SUS PROD: '||cntref);
            utl_file.put_line(log_file,dbtab||'SUS TEST: '||cnttst);
            utl_file.put_line(log_file,dbtab||'SUS DIFF: '||diff);
            utl_file.put_line(log_file,dbtab||' ');
            if diff <> 0 then ecart := 1 ;
            end if;
            
            if diff <> 0 then ecart := 1 ;
            end if;
            return ecart ;
        END verif_compte;
    
    procedure p010_quantieme is
        /*
            Récupération du quantième, a.k.a. la date du traitement
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p010_quantieme: Prise en compte du quantieme');
            SELECT quntm.QUANTIEME into dt_traitement FROM SYSTEME.QUANTIEME quntm ;
            utl_file.put_line(log_file,dbtab||'Quantieme: '|| dt_traitement);
            utl_file.put_line(log_file,ftab||'----------------------------------------------------');
        end p010_quantieme ;

    procedure p020_trunc_event_client_tmp is
        /*
            Vidage de la table tampon
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p020_trunc_event_client_tmp: Purge de la table "systeme.evenements_clients_tmp"');
            delete from systeme.evenements_clients_tmp;
            utl_file.put_line(log_file,dbtab||'Ligne supprimé: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||'----------------------------------------------------');
        end p020_trunc_event_client_tmp;

    procedure p030_sygal is
        dt_evenement date := null;
        countloop int := 0;
        typetier number(1) := null;
        dt_rep date := null;
        /*
            Récupération des avis de Sygal, mise en forme et traitement des données, puis insertion dans la table tampon
        */
        cursor crsr_sygal is
            SELECT 
                cd_offrrefe as cd_ref_offre,
                cd_typeavis as cd_type_avis,
                mt_prixvent/100 as mt_prix_vente,
                to_date(dt_avis,'yyyymmdd') as dt_avis,
                decode(dt_comm,'00000000',null,to_date(dt_comm,'yyyymmdd')) as dt_comm,
                decode(dt_susp,'00000000',null,to_date(dt_susp,'yyyymmdd')) as dt_susp,
                nm_chroclie as nm_chrono_client,
                cd_clecontclie as cd_cle_cont_client,
                nm_commclie as nm_comm_client,
                cd_prod as cd_produit,
                decode(trim(cd_inse),'',substr(lb_adredest,191,5), cd_inse) as cd_insee_client,
                trim(substr(lb_adredest,1,38)) as v1_client,
                trim(substr(lb_adredest,39,38)) as v2_client, 
                trim(trailing ' ' from substr(lb_adredest,77,38)) as v3_client,
                trim(trailing ' ' from substr(lb_adredest,115,38)) as v4_client,
                trim(substr(lb_adredest,153,38)) as v5_client,
                substr(lb_adredest,191,5) as cd_post_client,  
                trim(trailing ' ' from substr(lb_adredest,196,32)) as ville_client,
                decode(trim(cd_soci),'',1,cd_soci) as cd_societe,
                decode(nm_chrocliedona,'00000000','',nm_chrocliedona) as nm_chrono_tier,
                decode(nm_ligncommclie,'',0,nm_ligncommclie) as nm_ligne_comm_client,
                decode(trim(cd_camp),'',null,cd_camp) as cd_camp,
                decode(trim(cd_typelist),'',null,cd_typelist) as cd_type_liste,
                decode(trim(tx_comm),'',0,tx_comm) as tx_comm,
                decode(trim(cd_natuarti),'',null,cd_natuarti) as cd_nature_article,
                trim(bl_exte) as fg_exte,
                decode(trim(cd_typeannu),'',null,cd_typeannu) as cd_type_annulation,
                decode(dt_finchadprov,'00000000',null,to_date(dt_finchadprov,'yyyymmdd')) as dt_fin_changement_adrs_prov,
                trim(substr(lb_adredona,1,38)) as v1_tier,
                trim(substr(lb_adredona,39,38)) as v2_tier,
                trim(trailing ' ' from substr(lb_adredona,77,38)) as v3_tier,
                trim(trailing ' ' from substr(lb_adredona,115,38)) as v4_tier,
                trim(substr(lb_adredona,153,38)) as v5_tier,
                substr(lb_adredona,191,5) as cd_post_tier, 
                trim(trailing ' ' from substr(lb_adredona,196,32)) as ville_tier,
                decode(cd_typeoffr,'MAR','PRI',cd_typeoffr) as cd_type_offre,
                nm_envo as nm_envoi,
                decode(trim(cm_envo),'',null,cm_envo) as comm_envoi,
                decode(nm_quancol,'',0,nm_quancol) as nm_quant_collection,
                decode(id_ligncomm,'',null,id_ligncomm) as id_ligne_comm,
                decode(nm_paru,'0000000000',null,lpad(nm_paru,10,'')) as nm_parution,
                decode(dt_plan,null,null, dt_plan) as dt_planifier,
                lb_mail as lb_email,
                nm_quan,
                id_avis
            FROM avis@sygal WHERE fg_transyse = 2;
    begin
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p050_sygal: Extraction de "avis@sygal" vers "systeme.evenements_clients_tmp"');
        for sygal in crsr_sygal 
        loop
            if sygal.cd_type_avis = 'RES' or sygal.cd_type_avis = 'CHD' OR sygal.cd_type_avis = 'SUS' then
                dt_evenement := sygal.dt_susp;
            else
                dt_evenement := sygal.dt_avis;
            end if;

            if sygal.cd_type_avis = 'CHD' or sygal.cd_type_avis = 'CHF' then
                dt_rep := sygal.dt_susp ;
            else
                dt_rep := null ;
            end if;

            if (sygal.cd_type_avis = 'RES' or sygal.cd_type_avis = 'CHD' or sygal.cd_type_avis = 'SUS' or sygal.cd_type_avis = 'CHF') then 
                sygal.dt_susp := sygal.dt_fin_changement_adrs_prov;
            end if;

            if (sygal.cd_type_offre = 'MAR' or sygal.cd_type_offre = 'PRI') and sygal.cd_produit < 10000 then 
                sygal.cd_produit := 10000 + sygal.cd_produit;
            end if;

            if sygal.cd_type_offre <> 'ADL' then 
                sygal.nm_parution := null;
            end if;

            if sygal.cd_type_offre <> 'ADL' then 
                sygal.dt_planifier := null;
            end if;

            if trim(sygal.v3_tier) = trim(sygal.v3_client) or trim(sygal.cd_post_tier) = trim(sygal.cd_post_client) then 
                typetier := null;
            else
                typetier := 1;
            end if;

            insert into systeme.evenements_clients_tmp(
                id_evenement,
                type_offre,
                systeme_vente,
                datefabrication,
                dateacquisition_fichier,
                date_commande,
                date_evenement,
                letchrono,
                chronocli,
                inseeclient,
                paysclient,
                v1client,
                v2client,
                v3client,
                v4client,
                cpostclient,
                villeclient,
                emailclient,
                telclient,
                typetiers,
                lettiers,
                chronotiers,
                paystiers,
                inseetiers,
                v1tiers,
                v2tiers,
                v3tiers,
                v4tiers,
                cposttiers,
                villetiers,
                emailtiers,
                teltiers,
                dateeffet,
                datereprise,
                code_article,
                type_produit,
                codefpartenaire,
                numcommande,
                nbnum,
                codemailing,
                codestatut,
                datestatut,
                chrono_dest,
                code_produit,
                chronoediteur,
                chronofull,
                societe_vente,
                date_planning,
                num_ordre_type_evenement,
                date_expedition_parametre,
                chrono_presta,
                date_susp_rep_ann,
                num_disquette,
                nb_disquette,
                refection,
                id_evenement_lien,
                code_campagne,
                numero_abo,
                type_liste,
                taux_comm,
                nature_article,
                flag_extension,
                type_annulation,
                type_evenement,
                support_refection,
                montant_article,
                v5client,
                v5tiers,
                nm_envoi,
                lb_envoi,
                qte_expediee,
                id_ligncomm,
                nm_paru,
                dt_plan,
                id_commweb,
                lb_refeextecde
            )
            values(
                systeme.seq_evenement_client.nextval,
                sygal.cd_type_offre,
                'SYG',
                null,
                dt_traitement,
                sygal.dt_comm,
                dt_evenement,
                sygal.cd_cle_cont_client,
                sygal.nm_chrono_client,
                sygal.cd_insee_client,
                null,
                sygal.v1_client,
                sygal.v2_client,
                sygal.v3_client,
                sygal.v4_client,
                sygal.cd_post_client,
                sygal.ville_client,
                sygal.lb_email,
                null,
                typetier,
                null,
                sygal.nm_chrono_tier,
                null,
                null,
                sygal.v1_tier,
                sygal.v2_tier,
                sygal.v3_tier,
                sygal.v4_tier,
                sygal.cd_post_tier,
                sygal.ville_tier,
                null,
                null,
                null,
                dt_rep,
                sygal.cd_ref_offre,
                'ERR',
                '001',
                sygal.nm_comm_client,
                sygal.nm_quan,
                null,
                0,
                sysdate,
                'ERR',
                sygal.cd_produit,
                null,
                null,
                sygal.cd_societe,
                null,
                null,
                null,
                null,
                sygal.dt_susp, 
                null,
                null,
                0,
                null,
                sygal.cd_camp,
                sygal.nm_ligne_comm_client,
                sygal.cd_type_liste,
                sygal.tx_comm,
                sygal.cd_nature_article,
                sygal.fg_exte,
                sygal.cd_type_annulation,
                sygal.cd_type_avis,
                null,
                sygal.mt_prix_vente,
                sygal.v5_client,
                sygal.v5_tier,
                sygal.nm_envoi,
                sygal.comm_envoi,
                sygal.nm_quant_collection,
                sygal.id_ligne_comm,
                sygal.nm_parution,
                sygal.dt_planifier,
                null,
                null
            );
            update avis@sygal syg set dt_avis = to_char(sygal.dt_avis,'yyyymmdd'), dt_avisdate = trunc(SYSDATE), fg_transyse = 1 where syg.id_avis = sygal.id_avis ;
            countloop := countloop + 1;
        end loop;
        utl_file.put_line(log_file,dbtab||'itérations: '|| countloop);
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
    end p030_sygal;

    procedure p040_adlm is
        dt_evenement date := null;
        countloop int := 0;
        typetier number(1) := null;
        dt_rep date := null;
         /*
            Récupération des avis d'Adl Master, mise en forme et traitement des données, puis insertion dans la table tampon
        */
        cursor crsr_adlm is
            SELECT 
                cd_fichpart as codefpartenaire,
                cd_offrrefe as cd_ref_offre,
                cd_typeavis as cd_type_avis,
                mt_prixvent/100 as mt_prix_vente,
                to_date(dt_avis,'yyyymmdd') as dt_avis,
                decode(dt_comm,'00000000',null,to_date(dt_comm,'yyyymmdd')) as dt_comm,
                decode(dt_susp,'00000000',null,to_date(dt_susp,'yyyymmdd')) as dt_susp,
                nm_chroclie as nm_chrono_client,
                cd_clecontclie as cd_cle_cont_client,
                nm_commclie as nm_comm_client,
                cd_prod as cd_produit,
                decode(trim(cd_inse),'',substr(lb_adredest,191,5), cd_inse) as cd_insee_client,
                trim(substr(lb_adredest,1,38)) as v1_client,
                trim(substr(lb_adredest,39,38)) as v2_client, 
                trim(trailing ' ' from substr(lb_adredest,77,38)) as v3_client,
                trim(trailing ' ' from substr(lb_adredest,115,38)) as v4_client,
                trim(substr(lb_adredest,153,38)) as v5_client,
                substr(lb_adredest,191,5) as cd_post_client,  
                trim(trailing ' ' from substr(lb_adredest,196,32)) as ville_client,
                decode(trim(cd_soci),'',1,cd_soci) as cd_societe,
                decode(nm_chrocliedona,'00000000','',nm_chrocliedona) as nm_chrono_tier,
                decode(nm_ligncommclie,'',0,nm_ligncommclie) as nm_ligne_comm_client,
                decode(trim(cd_camp),'',null,cd_camp) as cd_camp,
                decode(trim(cd_typelist),'',null,cd_typelist) as cd_type_liste,
                decode(trim(tx_comm),'',0,tx_comm) as tx_comm,
                decode(trim(cd_natuarti),'',null,cd_natuarti) as cd_nature_article,
                trim(bl_exte) as fg_exte,
                decode(trim(cd_typeannu),'',null,cd_typeannu) as cd_type_annulation,
                decode(dt_finchadprov,'00000000',null,to_date(dt_finchadprov,'yyyymmdd')) as dt_fin_changement_adrs_prov,
                trim(substr(lb_adredona,1,38)) as v1_tier,
                trim(substr(lb_adredona,39,38)) as v2_tier,
                trim(trailing ' ' from substr(lb_adredona,77,38)) as v3_tier,
                trim(trailing ' ' from substr(lb_adredona,115,38)) as v4_tier,
                trim(substr(lb_adredona,153,38)) as v5_tier,
                substr(lb_adredona,191,5) as cd_post_tier, 
                trim(trailing ' ' from substr(lb_adredona,196,32)) as ville_tier,
                decode(cd_typeoffr,'MAR','PRI',cd_typeoffr) as cd_type_offre,
                nm_envo as nm_envoi,
                decode(trim(cm_envo),'',null,cm_envo) as comm_envoi,
                decode(nm_quancol,'',0,nm_quancol) as nm_quant_collection,
                decode(id_ligncomm,'',null,id_ligncomm) as id_ligne_comm,
                decode(lb_numeplan,'0000000000',null,trim(lb_numeplan)) as nm_parution,
                decode(dt_plan,'00000000',null,'',null, to_date(dt_plan,'yyyymmdd')) as dt_planifier,
                lb_mail as lb_email,
                decode(trim(nm_quan),'',null,to_number(nm_quan,'999')) as nm_quan,
                id_avis
            FROM avis@adlm WHERE fg_transyse = 2;
    begin
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p060_adlm: Exctraction de "avis@adlm" vers "systeme.evenements_clients_tmp"');
		for avis_adlm in crsr_adlm 
		loop
            if avis_adlm.cd_type_avis = 'RES' or avis_adlm.cd_type_avis = 'CHD' OR avis_adlm.cd_type_avis = 'SUS' then
                dt_evenement := avis_adlm.dt_susp;
            else
                dt_evenement := avis_adlm.dt_avis;
            end if;
            
            if avis_adlm.cd_type_avis = 'CHD' or avis_adlm.cd_type_avis = 'CHF' then
                dt_rep := avis_adlm.dt_susp ;
            else
                dt_rep := null ;
            end if;

            if (avis_adlm.cd_type_avis = 'RES' or avis_adlm.cd_type_avis = 'CHD' or avis_adlm.cd_type_avis = 'SUS' or avis_adlm.cd_type_avis = 'CHF') then 
                avis_adlm.dt_susp := avis_adlm.dt_fin_changement_adrs_prov ;
            end if;

            if (avis_adlm.cd_type_offre = 'MAR' or avis_adlm.cd_type_offre = 'PRI') and avis_adlm.cd_produit < 10000 then 
                avis_adlm.cd_produit := 10000 + avis_adlm.cd_produit;
            end if;

            if avis_adlm.cd_type_offre <> 'ADL' then 
                avis_adlm.dt_planifier := null;
            end if;
            
            if trim(avis_adlm.v3_tier) = trim(avis_adlm.v3_client) or trim(avis_adlm.cd_post_tier) = trim(avis_adlm.cd_post_client) then 
                typetier := null;
            else
                typetier := 1;
            end if;

			insert into systeme.evenements_clients_tmp(
				id_evenement,
				type_offre,
				systeme_vente,
				datefabrication,
				dateacquisition_fichier,
				date_commande,
				date_evenement,
				letchrono,
				chronocli,
				inseeclient,
				paysclient,
				v1client,
				v2client,
				v3client,
				v4client,
				cpostclient,
				villeclient,
				emailclient,
				telclient,
				typetiers,
				lettiers,
				chronotiers,
				paystiers,
				inseetiers,
				v1tiers,
				v2tiers,
				v3tiers,
				v4tiers,
				cposttiers,
				villetiers,
				emailtiers,
				teltiers,
				dateeffet,
				datereprise,
				code_article,
                type_produit,
				codefpartenaire,
				numcommande,
				nbnum,
				codemailing,
				codestatut,
				datestatut,
				chrono_dest,
				code_produit,
				chronoediteur,
				chronofull,
				societe_vente,
				date_planning,
				num_ordre_type_evenement,
				date_expedition_parametre,
				chrono_presta,
				date_susp_rep_ann,
				num_disquette,
				nb_disquette,
				refection,
				id_evenement_lien,
				code_campagne,
				numero_abo,
				type_liste,
				taux_comm,
                nature_article,
				flag_extension,
				type_annulation,
				type_evenement,
                support_refection,
				montant_article,
				v5client,
				v5tiers,
				nm_envoi,
				lb_envoi,
				qte_expediee,
				id_ligncomm,
                id_commweb,
				lb_refeextecde,
                dt_plan,
                nm_paru
			)
			values(
				systeme.seq_evenement_client.nextval,
				avis_adlm.cd_type_offre,
				'ADM',
				null,
				dt_traitement,
				avis_adlm.dt_comm,
                dt_evenement,
				avis_adlm.cd_cle_cont_client,
				avis_adlm.nm_chrono_client,
				avis_adlm.cd_insee_client,
				null,
				avis_adlm.v1_client,
				avis_adlm.v2_client,
				avis_adlm.v3_client,
				avis_adlm.v4_client,
				avis_adlm.cd_post_client,
				avis_adlm.ville_client,
				avis_adlm.lb_email,
				null,
				typetier,
				null,
				avis_adlm.nm_chrono_tier,
				null,
				null,
				avis_adlm.v1_tier,
				avis_adlm.v2_tier,
				avis_adlm.v3_tier,
				avis_adlm.v4_tier,
				avis_adlm.cd_post_tier,
                avis_adlm.ville_tier,
				null,
				null,
				null,
				dt_rep,
				avis_adlm.cd_ref_offre,
				'ERR',
                avis_adlm.codefpartenaire,
				avis_adlm.nm_comm_client,
				avis_adlm.nm_quan,
				null,
				0,
				sysdate,
				'ERR',
				avis_adlm.cd_produit,
				null,
				null,
				avis_adlm.cd_societe,
				null,
				null,
				null,
				null,
				avis_adlm.dt_susp,
				null,
				null,
				0,
				null,
				avis_adlm.cd_camp,
				avis_adlm.nm_ligne_comm_client,
				avis_adlm.cd_type_liste,
				avis_adlm.tx_comm,
				avis_adlm.cd_nature_article,
                avis_adlm.fg_exte,
				avis_adlm.cd_type_annulation,
				avis_adlm.cd_type_avis,
				null,
                avis_adlm.mt_prix_vente,
				avis_adlm.v5_client,
				avis_adlm.v5_tier,
				avis_adlm.nm_envoi,
				avis_adlm.comm_envoi,
				avis_adlm.nm_quant_collection,
				avis_adlm.id_ligne_comm,
				null,
				null,
                avis_adlm.dt_planifier,
                avis_adlm.nm_parution
			);
            update avis@adlm adlm set dt_avis = to_char(avis_adlm.dt_avis,'yyyymmdd'), dt_avisdate = trunc(SYSDATE), fg_transyse = 1 where adlm.id_avis = avis_adlm.id_avis ;
            countloop := countloop + 1;
		end loop;
        utl_file.put_line(log_file,dbtab||'Itérations: '|| countloop);
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
    end p040_adlm;

    procedure p050_maj_ec_tmp_add_adl is
         /*
            Mise en forme des noms et prenoms, détection des emails défectueux (regex)
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p070_maj_ec_tmp_add_adl: mise en forme des V1 client et vérification des emails');
            
            utl_file.put_line(log_file,dbtab||'Mise en forme des V1:');
            update systeme.evenements_clients_tmp tmp set tmp.V1CLIENT = replace(tmp.V1CLIENT,'.',''),  tmp.V1TIERS  = replace(tmp.V1TIERS ,'.','') where (tmp.V1CLIENT like '%.%' or tmp.V1TIERS like '%.%');
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            
            -- vérification email client
            utl_file.put_line(log_file,dbtab||'Email client vérifiés:');
            update systeme.evenements_clients_tmp tmp set tmp.EMAILCLIENT = null where tmp.EMAILCLIENT is not null and not exists (
                select 1 from dual where REGEXP_LIKE (tmp.EMAILCLIENT, '^(([a-zA-Z0-9_\-\.-]+)@([a-zA-Z0-9_\-\.-]+)\.([a-zA-Z]{2,5}){1,25})$')
            );
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            
            -- vérification email tiers
            utl_file.put_line(log_file,dbtab||'Email tiers vérifiés');
            update systeme.evenements_clients_tmp tmp set tmp.EMAILTIERS = null where tmp.EMAILTIERS is not null and not exists (
                select 1 from dual where REGEXP_LIKE (tmp.EMAILTIERS, '^(([a-zA-Z0-9_\-\.-]+)@([a-zA-Z0-9_\-\.-]+)\.([a-zA-Z]{2,5}){1,25})$')
            );
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p050_maj_ec_tmp_add_adl;

    procedure p060_ann_an3_74 is
        /*
            Transformation de certaines annulations (ANN) en annulation 3 mois (AN3)
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p080_ann_an3_74: transformation des annulations en annulation 3 mois.');
            UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client_tmp SET event_client_tmp.TYPE_EVENEMENT = 'AN3' WHERE (SUBSTR(event_client_tmp.CHRONOCLI,1,1) = '6' AND event_client_tmp.TYPE_EVENEMENT = 'ANN' AND event_client_tmp.CODE_CAMPAGNE = 74 AND NOT (event_client_tmp.DATE_EXPEDITION_PARAMETRE IS NULL));
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p060_ann_an3_74;

     procedure p070_code_statut0 is
        countloop int := 0;
        typetier number(1);
        dt_rep date := null;
         /*
            Récupération des avis qui n'ont pas encore été envoyé.
        */
        cursor crsr_codestat0 is
            SELECT 
                event_client.ID_EVENEMENT as id_evenement, 
                event_client.TYPE_OFFRE as type_offre, 
                event_client.SYSTEME_VENTE as systeme_vente, 
                event_client.DATEFABRICATION as dt_fab, 
                event_client.DATEACQUISITION_FICHIER as dt_acqu, 
                event_client.DATE_COMMANDE as dt_comm, 
                event_client.DATE_EVENEMENT as dt_event, 
                event_client.LETCHRONO as letchrono, 
                event_client.CHRONOCLI as chrono_client, 
                event_client.INSEECLIENT as insee_client, 
                event_client.PAYSCLIENT as pays_client, 
                event_client.V1CLIENT as v1_client, 
                event_client.V2CLIENT as v2_client, 
                event_client.V3CLIENT as v3_client, 
                event_client.V4CLIENT as v4_client, 
                event_client.CPOSTCLIENT as cd_post_client, 
                event_client.VILLECLIENT as ville_client, 
                event_client.EMAILCLIENT as email_client, 
                event_client.TELCLIENT as tel_client, 
                event_client.TYPETIERS as type_tiers, 
                event_client.LETTIERS as lettiers, 
                event_client.CHRONOTIERS as chrono_tiers, 
                event_client.PAYSTIERS as pays_tiers, 
                event_client.INSEETIERS as insee_tiers, 
                event_client.V1TIERS as v1_tiers, 
                event_client.V2TIERS as V2_tiers, 
                event_client.V3TIERS as v3_tiers, 
                event_client.V4TIERS as v4_tiers, 
                event_client.CPOSTTIERS as cd_post_tiers, 
                event_client.VILLETIERS as ville_tiers, 
                event_client.EMAILTIERS as email_tiers, 
                event_client.TELTIERS as tel_tiers, 
                event_client.DATEEFFET as dt_effet, 
                event_client.DATEREPRISE as dt_reprise, 
                event_client.CODE_ARTICLE as cd_article, 
                event_client.TYPE_PRODUIT as type_produit, 
                event_client.CODEFPARTENAIRE as cd_partenaire, 
                event_client.NUMCOMMANDE as nm_comm, 
                event_client.NBNUM as nm_numero, 
                event_client.CODEMAILING as cd_mail, 
                event_client.CODESTATUT as cd_statut, 
                event_client.DATESTATUT as dt_statut, 
                event_client.CHRONO_DEST as chrono_dest, 
                event_client.CODE_PRODUIT as cd_produit, 
                event_client.CHRONOEDITEUR as chrono_editeur, 
                event_client.CHRONOFULL as chrono_full, 
                event_client.SOCIETE_VENTE as societe_vente, 
                event_client.DATE_PLANNING as dt_planning, 
                event_client.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
                event_client.DATE_EXPEDITION_PARAMETRE as dt_exped_param, 
                event_client.CHRONO_PRESTA as chrono_presta, 
                event_client.DATE_SUSP_REP_ANN as dt_susp_rep_ann, 
                event_client.NUM_DISQUETTE as nm_disc, 
                event_client.NB_DISQUETTE as nb_disc, 
                event_client.REFECTION as refection, 
                event_client.ID_EVENEMENT_LIEN as id_event_lien, 
                event_client.CODE_CAMPAGNE as code_camp, 
                event_client.NUMERO_ABO as nm_abo, 
                event_client.TYPE_LISTE as type_liste, 
                event_client.TAUX_COMM as tx_comm, 
                event_client.NATURE_ARTICLE as nature_article, 
                event_client.FLAG_EXTENSION as fg_exte, 
                event_client.TYPE_ANNULATION as type_annulation, 
                event_client.TYPE_EVENEMENT as type_event, 
                event_client.SUPPORT_REFECTION as supp_ref, 
                event_client.MONTANT_ARTICLE as mt_article, 
                event_client.V5CLIENT as v5_client, 
                event_client.V5TIERS as v5_tiers, 
                event_client.NM_ENVOI as nm_envoi, 
                event_client.LB_ENVOI as lb_envoi,
                event_client.QTE_EXPEDIEE as quant_expe, 
                event_client.ID_LIGNCOMM as id_ligne_comm, 
                event_client.NM_PARU as nm_parution, 
                event_client.DT_PLAN as dt_planifier, 
                event_client.ID_COMMWEB as id_commweb, 
                event_client.LB_REFEEXTECDE as lb_ref_exte_code 
            FROM SYSTEME.EVENEMENTS_CLIENTS event_client WHERE event_client.CODESTATUT = 0  AND event_client.TYPE_EVENEMENT <> 'MES' ;
    begin
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p090_code_statut0: récupération des avis code_statut = 0 de "systeme.evenements_clients" vers "systeme.evenements_clients_tmp"');
        for avisstat0 in crsr_codestat0
		loop
            begin
                INSERT INTO SYSTEME.EVENEMENTS_CLIENTS_TMP (
                    ID_EVENEMENT,
                    TYPE_OFFRE,
                    SYSTEME_VENTE,
                    DATEFABRICATION,
                    DATEACQUISITION_FICHIER,
                    DATE_COMMANDE,
                    DATE_EVENEMENT,
                    LETCHRONO,
                    CHRONOCLI,
                    INSEECLIENT,
                    PAYSCLIENT,
                    V1CLIENT,
                    V2CLIENT,
                    V3CLIENT,
                    V4CLIENT,
                    CPOSTCLIENT,
                    VILLECLIENT,
                    EMAILCLIENT,
                    TELCLIENT,
                    TYPETIERS,
                    LETTIERS,
                    CHRONOTIERS,
                    PAYSTIERS,
                    INSEETIERS,
                    V1TIERS,
                    V2TIERS,
                    V3TIERS,
                    V4TIERS,
                    CPOSTTIERS,
                    VILLETIERS,
                    EMAILTIERS,
                    TELTIERS,
                    DATEEFFET,
                    DATEREPRISE,
                    CODE_ARTICLE,
                    TYPE_PRODUIT,
                    CODEFPARTENAIRE,
                    NUMCOMMANDE,
                    NBNUM,
                    CODEMAILING,
                    CODESTATUT,
                    DATESTATUT,
                    CHRONO_DEST,
                    CODE_PRODUIT,
                    CHRONOEDITEUR,
                    CHRONOFULL,
                    SOCIETE_VENTE,
                    DATE_PLANNING,
                    NUM_ORDRE_TYPE_EVENEMENT,
                    DATE_EXPEDITION_PARAMETRE,
                    CHRONO_PRESTA,
                    DATE_SUSP_REP_ANN,
                    NUM_DISQUETTE,
                    NB_DISQUETTE,
                    REFECTION,
                    ID_EVENEMENT_LIEN,
                    CODE_CAMPAGNE,
                    NUMERO_ABO,
                    TYPE_LISTE,
                    TAUX_COMM,
                    NATURE_ARTICLE,
                    FLAG_EXTENSION,
                    TYPE_ANNULATION,
                    TYPE_EVENEMENT,
                    SUPPORT_REFECTION,
                    MONTANT_ARTICLE,
                    V5CLIENT,
                    V5TIERS,
                    NM_ENVOI,
                    LB_ENVOI,
                    QTE_EXPEDIEE,
                    ID_LIGNCOMM,
                    NM_PARU,
                    DT_PLAN,
                    ID_COMMWEB,
                    LB_REFEEXTECDE
                ) 
                VALUES (
                    avisstat0.ID_EVENEMENT,
                    avisstat0.TYPE_OFFRE,
                    avisstat0.SYSTEME_VENTE,
                    avisstat0.DT_FAB,
                    avisstat0.DT_ACQU,
                    avisstat0.DT_COMM,
                    avisstat0.DT_EVENT,
                    avisstat0.LETCHRONO,
                    avisstat0.CHRONO_CLIENT,
                    avisstat0.INSEE_CLIENT,
                    avisstat0.PAYS_CLIENT,
                    avisstat0.V1_CLIENT,
                    avisstat0.V2_CLIENT,
                    avisstat0.V3_CLIENT,
                    avisstat0.V4_CLIENT,
                    avisstat0.cd_POST_CLIENT,
                    avisstat0.VILLE_CLIENT,
                    avisstat0.EMAIL_CLIENT,
                    avisstat0.TEL_CLIENT,
                    avisstat0.TYPE_TIERS,
                    avisstat0.LETTIERS,
                    avisstat0.CHRONO_TIERS,
                    avisstat0.PAYS_TIERS,
                    avisstat0.INSEE_TIERS,
                    avisstat0.V1_TIERS,
                    avisstat0.V2_TIERS,
                    avisstat0.V3_TIERS,
                    avisstat0.V4_TIERS,
                    avisstat0.cd_POST_TIERS,
                    avisstat0.VILLE_TIERS,
                    avisstat0.EMAIL_TIERS,
                    avisstat0.TEL_TIERS,
                    avisstat0.DT_EFFET,
                    avisstat0.DT_REPRISE,
                    avisstat0.CD_ARTICLE,
                    avisstat0.TYPE_PRODUIT,
                    avisstat0.CD_PARTENAIRE,
                    avisstat0.NM_COMM,
                    avisstat0.NM_NUMERO,
                    avisstat0.CD_MAIL,
                    avisstat0.CD_STATUT,
                    avisstat0.DT_STATUT,
                    avisstat0.CHRONO_DEST,
                    avisstat0.CD_PRODUIT,
                    avisstat0.CHRONO_EDITEUR,
                    avisstat0.CHRONO_FULL,
                    avisstat0.SOCIETE_VENTE,
                    avisstat0.DT_PLANNING,
                    avisstat0.NM_ORDRE_TYPE_EVENT,
                    avisstat0.DT_EXPED_PARAM,
                    avisstat0.CHRONO_PRESTA,
                    avisstat0.DT_SUSP_REP_ANN,
                    avisstat0.NM_DISC,
                    avisstat0.NB_DISC,
                    avisstat0.REFECTION,
                    avisstat0.ID_EVENT_LIEN,
                    avisstat0.CODE_CAMP,
                    avisstat0.NM_ABO,
                    avisstat0.TYPE_LISTE,
                    avisstat0.TX_COMM,
                    avisstat0.NATURE_ARTICLE,
                    avisstat0.FG_EXTE,
                    avisstat0.TYPE_ANNULATION,
                    avisstat0.TYPE_EVENT,
                    avisstat0.SUPP_REF,
                    avisstat0.MT_ARTICLE,
                    avisstat0.V5_CLIENT,
                    avisstat0.V5_TIERS,
                    avisstat0.NM_ENVOI,
                    avisstat0.LB_ENVOI,
                    avisstat0.QUANT_EXPE,
                    avisstat0.ID_LIGNE_COMM,
                    avisstat0.NM_PARUTION,
                    avisstat0.dt_planifier,
                    avisstat0.ID_COMMWEB,
                    avisstat0.LB_REF_EXTE_CODE
                );
            end;
            countloop := countloop + 1;
		end loop;
        utl_file.put_line(log_file,dbtab||'itérations: '|| countloop);
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');

    end p070_code_statut0 ;

    procedure p080_150_annulation_periode is
        countloop int := 0;
        typetier number(1);
        dt_rep date := null;
        deleted int := 0  ;
        /*
            transfert des annulations de la table tampon vers la table annulation_période
        */
        cursor crsr_annulation is
            SELECT 
                v_ann_per.ID_EVENEMENT as id_evenement, 
                v_ann_per.TYPE_OFFRE as type_offre, 
                v_ann_per.SYSTEME_VENTE as systeme_vente, 
                v_ann_per.DATEFABRICATION as date_fab, 
                v_ann_per.DATEACQUISITION_FICHIER as date_acqu, 
                v_ann_per.DATE_COMMANDE as date_comm, 
                v_ann_per.DATE_EVENEMENT as date_event, 
                v_ann_per.LETCHRONO as letchrono, 
                v_ann_per.CHRONOCLI as chrono_client, 
                v_ann_per.INSEECLIENT as insee_client, 
                v_ann_per.PAYSCLIENT as pays_client, 
                v_ann_per.V1CLIENT as v1_client, 
                v_ann_per.V2CLIENT as v2_client, 
                v_ann_per.V3CLIENT as v3_client, 
                v_ann_per.V4CLIENT as v4_client, 
                v_ann_per.CPOSTCLIENT as code_post_client, 
                v_ann_per.VILLECLIENT as ville_client, 
                v_ann_per.EMAILCLIENT as email_client, 
                v_ann_per.TELCLIENT as tel_client, 
                v_ann_per.TYPETIERS as type_tiers, 
                v_ann_per.LETTIERS as lettiers, 
                v_ann_per.CHRONOTIERS as chrono_tiers, 
                v_ann_per.PAYSTIERS as pays_tiers, 
                v_ann_per.INSEETIERS as insee_tiers, 
                v_ann_per.V1TIERS as v1_tiers, 
                v_ann_per.V2TIERS as V2_tiers, 
                v_ann_per.V3TIERS as v3_tiers, 
                v_ann_per.V4TIERS as v4_tiers, 
                v_ann_per.CPOSTTIERS as code_post_tiers, 
                v_ann_per.VILLETIERS as ville_tiers, 
                v_ann_per.EMAILTIERS as email_tiers, 
                v_ann_per.TELTIERS as tel_tiers, 
                v_ann_per.DATEEFFET as date_effet, 
                v_ann_per.DATEREPRISE as date_reprise, 
                v_ann_per.CODE_ARTICLE as code_article, 
                v_ann_per.TYPE_PRODUIT as type_produit, 
                v_ann_per.CODEFPARTENAIRE as code_partenaire, 
                v_ann_per.NUMCOMMANDE as num_commande, 
                v_ann_per.NBNUM as nombre_numero, 
                v_ann_per.CODEMAILING as code_mail, 
                v_ann_per.CODESTATUT as code_statut, 
                v_ann_per.DATESTATUT as date_statut, 
                v_ann_per.CHRONO_DEST as chrono_dest, 
                v_ann_per.CODE_PRODUIT as code_produit, 
                v_ann_per.CHRONOEDITEUR as chrono_editeur, 
                v_ann_per.CHRONOFULL as chrono_full, 
                v_ann_per.SOCIETE_VENTE as societe_vente, 
                v_ann_per.DATE_PLANNING as date_planning, 
                v_ann_per.NUM_ORDRE_TYPE_EVENEMENT as numero_ordre_type_event, 
                v_ann_per.DATE_EXPEDITION_PARAMETRE as date_exped_param, 
                v_ann_per.CHRONO_PRESTA as chrono_presta, 
                v_ann_per.DATE_SUSP_REP_ANN as susp_rep_ann, 
                v_ann_per.NUM_DISQUETTE as nm_disc, 
                v_ann_per.NB_DISQUETTE as nb_disc, 
                v_ann_per.REFECTION as refection, 
                v_ann_per.ID_EVENEMENT_LIEN as id_event_lien, 
                v_ann_per.CODE_CAMPAGNE as code_camp, 
                v_ann_per.NUMERO_ABO as nm_abo, 
                v_ann_per.TYPE_LISTE as type_liste, 
                v_ann_per.TAUX_COMM as tx_comm, 
                v_ann_per.NATURE_ARTICLE as nature_article, 
                v_ann_per.FLAG_EXTENSION as flag_exte, 
                v_ann_per.TYPE_ANNULATION as type_annulation, 
                v_ann_per.TYPE_EVENEMENT as type_event, 
                v_ann_per.SUPPORT_REFECTION as supp_ref, 
                v_ann_per.MONTANT_ARTICLE as mt_article, 
                v_ann_per.V5CLIENT as v5_client, 
                v_ann_per.V5TIERS as v5_tiers, 
                v_ann_per.NM_ENVOI as numero_envoi, 
                v_ann_per.LB_ENVOI as libelle_envoi,
                v_ann_per.QTE_EXPEDIEE as quant_expe, 
                v_ann_per.ID_LIGNCOMM as id_ligne_commande, 
                v_ann_per.NM_PARU as numero_de_parution, 
                v_ann_per.DT_PLAN as date_planifier 
            FROM SYSTEME.ANNULATION_PERIODE_V v_ann_per;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p100_annulation_periode: récupération des avis de "systeme.annulation_periode_v" vers "systeme.annulation_periode"');
            for avis_annulation in crsr_annulation
            loop
                begin
                    INSERT INTO SYSTEME.ANNULATION_PERIODE (
                        ID_EVENEMENT,
                        TYPE_OFFRE,
                        SYSTEME_VENTE,
                        DATEACQUISITION_FICHIER,
                        DATE_COMMANDE,
                        DATE_EVENEMENT,
                        LETCHRONO,
                        CHRONOCLI,
                        INSEECLIENT,
                        PAYSCLIENT,
                        V1CLIENT,
                        V2CLIENT,
                        V3CLIENT,
                        V4CLIENT,
                        CPOSTCLIENT,
                        VILLECLIENT,
                        EMAILCLIENT,
                        TELCLIENT,
                        TYPETIERS,
                        LETTIERS,
                        CHRONOTIERS,
                        PAYSTIERS,
                        INSEETIERS,
                        V1TIERS,
                        V2TIERS,
                        V3TIERS,
                        V4TIERS,
                        CPOSTTIERS,
                        VILLETIERS,
                        EMAILTIERS,
                        TELTIERS,
                        DATEEFFET,
                        DATEREPRISE,
                        CODE_ARTICLE,
                        TYPE_PRODUIT,
                        CODEFPARTENAIRE,
                        NUMCOMMANDE,
                        NBNUM,
                        CODEMAILING,
                        CODESTATUT,
                        DATESTATUT,
                        CHRONO_DEST,
                        CODE_PRODUIT,
                        CHRONOEDITEUR,
                        CHRONOFULL,
                        SOCIETE_VENTE,
                        DATE_PLANNING,
                        NUM_ORDRE_TYPE_EVENEMENT,
                        DATE_EXPEDITION_PARAMETRE,
                        CHRONO_PRESTA,
                        DATE_SUSP_REP_ANN,
                        NUM_DISQUETTE,
                        NB_DISQUETTE,
                        REFECTION,
                        ID_EVENEMENT_LIEN,
                        CODE_CAMPAGNE,
                        NUMERO_ABO,			
                        TYPE_LISTE,
                        TAUX_COMM,
                        NATURE_ARTICLE,
                        FLAG_EXTENSION,
                        TYPE_ANNULATION,
                        TYPE_EVENEMENT,
                        SUPPORT_REFECTION,
                        MONTANT_ARTICLE,
                        V5CLIENT,
                        V5TIERS,
                        NM_ENVOI,
                        LB_ENVOI,
                        QTE_EXPEDIEE,
                        ID_LIGNCOMM,
                        NM_PARU,
                        DT_PLAN,
                        DT_INSR
                    ) 
                    VALUES (
                        avis_annulation.ID_EVENEMENT,
                        avis_annulation.TYPE_OFFRE,
                        avis_annulation.SYSTEME_VENTE,
                        avis_annulation.DATE_ACQU,
                        avis_annulation.DATE_COMM,
                        avis_annulation.DATE_EVENT,
                        avis_annulation.LETCHRONO,
                        avis_annulation.CHRONO_CLIENT,
                        avis_annulation.INSEE_CLIENT,
                        avis_annulation.PAYS_CLIENT,
                        avis_annulation.V1_CLIENT,
                        avis_annulation.V2_CLIENT,
                        avis_annulation.V3_CLIENT,
                        avis_annulation.V4_CLIENT,
                        avis_annulation.code_POST_CLIENT,
                        avis_annulation.VILLE_CLIENT,
                        avis_annulation.EMAIL_CLIENT,
                        avis_annulation.TEL_CLIENT,
                        avis_annulation.TYPE_TIERS,
                        avis_annulation.LETTIERS,
                        avis_annulation.CHRONO_TIERS,
                        avis_annulation.PAYS_TIERS,
                        avis_annulation.INSEE_TIERS,
                        avis_annulation.V1_TIERS,
                        avis_annulation.V2_TIERS,
                        avis_annulation.V3_TIERS,
                        avis_annulation.V4_TIERS,
                        avis_annulation.code_POST_TIERS,
                        avis_annulation.VILLE_TIERS,
                        avis_annulation.EMAIL_TIERS,
                        avis_annulation.TEL_TIERS,
                        avis_annulation.DATE_EFFET,
                        avis_annulation.DATE_REPRISE,
                        avis_annulation.CODE_ARTICLE,
                        avis_annulation.TYPE_PRODUIT,
                        avis_annulation.CODE_PARTENAIRE,
                        avis_annulation.NUM_COMMANDE,
                        avis_annulation.NOMBRE_NUMERO,
                        avis_annulation.CODE_MAIL,
                        avis_annulation.CODE_STATUT,
                        avis_annulation.DATE_STATUT,
                        avis_annulation.CHRONO_DEST,
                        avis_annulation.CODE_PRODUIT,
                        avis_annulation.CHRONO_EDITEUR,
                        avis_annulation.CHRONO_FULL,
                        avis_annulation.SOCIETE_VENTE,
                        avis_annulation.DATE_PLANNING,
                        avis_annulation.NUMERO_ORDRE_TYPE_EVENT,
                        avis_annulation.DATE_EXPED_PARAM,
                        avis_annulation.CHRONO_PRESTA,
                        avis_annulation.SUSP_REP_ANN,
                        avis_annulation.NM_DISC,
                        avis_annulation.NB_DISC,
                        avis_annulation.REFECTION,
                        avis_annulation.ID_EVENT_LIEN,
                        avis_annulation.CODE_CAMP,
                        avis_annulation.NM_ABO,
                        avis_annulation.TYPE_LISTE,
                        avis_annulation.TX_COMM,
                        avis_annulation.NATURE_ARTICLE,
                        avis_annulation.FLAG_EXTE,
                        avis_annulation.TYPE_ANNULATION,
                        avis_annulation.TYPE_EVENT,
                        avis_annulation.SUPP_REF,
                        avis_annulation.MT_ARTICLE,
                        avis_annulation.V5_CLIENT,
                        avis_annulation.V5_TIERS,
                        avis_annulation.NUMERO_ENVOI,
                        avis_annulation.LIBELLE_ENVOI,
                        avis_annulation.QUANT_EXPE,
                        avis_annulation.ID_LIGNE_COMMANDE,
                        avis_annulation.NUMERO_DE_PARUTION,
                        avis_annulation.date_planifier,
                        dt_traitement
                    );
                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                        utl_file.put_line(log_file,dbtab||'UNIQUE CONSTRAINT VIOLATION FOR: '||rtrn||dbtab||ftab||'ID EVENEMENT: '|| avis_annulation.id_evenement ||rtrn||dbtab||ftab||'SYSTEME DE VENTE: '|| avis_annulation.systeme_vente);
                        utl_file.fclose(log_file);
                        rollback;
                        RAISE DUP_VAL_ON_INDEX ;
                    end;
                delete from systeme.evenements_clients_tmp WHERE id_evenement =  avis_annulation.id_evenement;
                deleted := deleted + sql%rowcount ;
                countloop := countloop + 1;
            end loop;
            utl_file.put_line(log_file,dbtab||'Itérations: '|| countloop);
            utl_file.put_line(log_file,dbtab||'Supprimmé: '|| deleted);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p080_150_annulation_periode ;

    procedure p090_reinst_periode is
        /*
            Transferts des reinstallation dans annulation_periode
        */
        countloop int := 0;
        typetier number(1);
        dt_rep date := null;
        deleted int := null;
        cursor crsr_reisnt_per is
            SELECT
				reinst_per_v.ID_EVENEMENT as id_event, 
				reinst_per_v.TYPE_OFFRE as type_offre, 
				reinst_per_v.SYSTEME_VENTE as sys_vente, 
				reinst_per_v.DATEFABRICATION as dt_fab, 
				reinst_per_v.DATEACQUISITION_FICHIER as dt_acquis, 
				reinst_per_v.DATE_COMMANDE as dt_comm, 
				reinst_per_v.DATE_EVENEMENT as dt_event, 
				reinst_per_v.LETCHRONO as letchrono, 
				reinst_per_v.CHRONOCLI as chrono_client, 
				reinst_per_v.INSEECLIENT as insee_client, 
				reinst_per_v.PAYSCLIENT as pays_client, 
				reinst_per_v.V1CLIENT as v1_client, 
				reinst_per_v.V2CLIENT as v2_client, 
				reinst_per_v.V3CLIENT as v3_client, 
				reinst_per_v.V4CLIENT as v4_client, 
				reinst_per_v.V5CLIENT as v5_client, 
				reinst_per_v.CPOSTCLIENT as cd_post_client, 
				reinst_per_v.VILLECLIENT as ville_client, 
				reinst_per_v.EMAILCLIENT as email_client, 
				reinst_per_v.TELCLIENT as tel_client, 
				reinst_per_v.TYPETIERS as type_tiers, 
				reinst_per_v.LETTIERS as lettiers, 
				reinst_per_v.CHRONOTIERS as chrono_tiers, 
				reinst_per_v.PAYSTIERS as pays_tiers, 
				reinst_per_v.INSEETIERS as insee_tiers, 
				reinst_per_v.V1TIERS as v1_tiers, 
				reinst_per_v.V2TIERS as v2_tiers, 
				reinst_per_v.V3TIERS as v3_tiers, 
				reinst_per_v.V4TIERS as v4_tiers, 
				reinst_per_v.V5TIERS as v5_tiers, 
				reinst_per_v.CPOSTTIERS as cd_post_tiers, 
				reinst_per_v.VILLETIERS as ville_tiers, 
				reinst_per_v.EMAILTIERS as email_tiers, 
				reinst_per_v.TELTIERS as tel_tiers, 
				reinst_per_v.DATEEFFET as dt_effet, 
				reinst_per_v.DATEREPRISE as dt_rep, 
				reinst_per_v.CODE_ARTICLE as cd_article, 
				reinst_per_v.TYPE_PRODUIT as type_prod, 
				reinst_per_v.CODEFPARTENAIRE as cd_partenaire, 
				reinst_per_v.NUMCOMMANDE as nm_comm, 
				reinst_per_v.NBNUM as nb_numero, 
				reinst_per_v.CODEMAILING as cd_mailing, 
				reinst_per_v.CODESTATUT as cd_statut, 
				reinst_per_v.DATESTATUT as dt_statut, 
				reinst_per_v.CHRONO_DEST as chrono_dest, 
				reinst_per_v.CODE_PRODUIT as cd_produit, 
				reinst_per_v.CHRONOEDITEUR as chrono_edit, 
				reinst_per_v.CHRONOFULL as chrono_full, 
				reinst_per_v.SOCIETE_VENTE as soc_vente, 
				reinst_per_v.DATE_PLANNING as dt_plan, 
				reinst_per_v.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
				reinst_per_v.DATE_EXPEDITION_PARAMETRE as dt_exped_param, 
				reinst_per_v.CHRONO_PRESTA as chrono_presta, 
				reinst_per_v.DATE_SUSP_REP_ANN as dt_susp, 
				reinst_per_v.NUM_DISQUETTE as nm_disc, 
				reinst_per_v.NB_DISQUETTE as nb_disc, 
				reinst_per_v.REFECTION, 
				reinst_per_v.ID_EVENEMENT_LIEN as id_event_lien, 
				reinst_per_v.CODE_CAMPAGNE as cd_camp, 
				reinst_per_v.NUMERO_ABO as nm_abo, 
				reinst_per_v.TYPE_LISTE as type_liste, 
				reinst_per_v.TAUX_COMM as tx_comm, 
				reinst_per_v.NATURE_ARTICLE as nature_article, 
				reinst_per_v.FLAG_EXTENSION as fg_exte, 
				reinst_per_v.TYPE_ANNULATION as type_annulation, 
				reinst_per_v.TYPE_EVENEMENT as type_event, 
				reinst_per_v.SUPPORT_REFECTION as supp_refection, 
				reinst_per_v.MONTANT_ARTICLE as mt_article, 
				reinst_per_v.NM_ENVOI, 
				reinst_per_v.LB_ENVOI, 
				reinst_per_v.QTE_EXPEDIEE as quant_exped, 
				reinst_per_v.ID_LIGNCOMM as id_ligne_comm, 
				reinst_per_v.NM_PARU as nm_parution, 
				reinst_per_v.DT_PLAN as dt_planifier
			FROM SYSTEME.REINST_PERIODE_V reinst_per_v;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p110_reinst_periode: récupération des avis de "systeme.reinst_periode_v" vers "systeme.annulation_periode"');
            for avis_reinst_per in crsr_reisnt_per
            loop
                begin
                    INSERT INTO SYSTEME.ANNULATION_PERIODE (
                        ID_EVENEMENT,
                        TYPE_OFFRE,
                        SYSTEME_VENTE,
                        DATEFABRICATION,
                        DATEACQUISITION_FICHIER,
                        DATE_COMMANDE,
                        DATE_EVENEMENT,
                        LETCHRONO,
                        CHRONOCLI,	
                        INSEECLIENT,
                        PAYSCLIENT,
                        V1CLIENT,
                        V2CLIENT,
                        V3CLIENT,
                        V4CLIENT,
                        V5CLIENT,
                        CPOSTCLIENT,
                        VILLECLIENT,
                        EMAILCLIENT,
                        TELCLIENT,
                        TYPETIERS,
                        LETTIERS,
                        CHRONOTIERS,
                        PAYSTIERS,
                        INSEETIERS,
                        V1TIERS,
                        V2TIERS,
                        V3TIERS,
                        V4TIERS,
                        V5TIERS,
                        CPOSTTIERS,
                        VILLETIERS,
                        EMAILTIERS,
                        TELTIERS,
                        DATEEFFET,
                        DATEREPRISE,
                        CODE_ARTICLE,
                        TYPE_PRODUIT,
                        CODEFPARTENAIRE,
                        NUMCOMMANDE,
                        NBNUM,
                        CODEMAILING,
                        CODESTATUT,
                        DATESTATUT,
                        CHRONO_DEST,
                        CODE_PRODUIT,
                        CHRONOEDITEUR,
                        CHRONOFULL,
                        SOCIETE_VENTE,
                        DATE_PLANNING,
                        NUM_ORDRE_TYPE_EVENEMENT,
                        DATE_EXPEDITION_PARAMETRE,
                        CHRONO_PRESTA,
                        DATE_SUSP_REP_ANN,
                        NUM_DISQUETTE,
                        NB_DISQUETTE,
                        REFECTION,
                        ID_EVENEMENT_LIEN,
                        CODE_CAMPAGNE,
                        NUMERO_ABO,
                        TYPE_LISTE,
                        TAUX_COMM,
                        NATURE_ARTICLE,
                        FLAG_EXTENSION,
                        TYPE_ANNULATION,
                        TYPE_EVENEMENT,
                        SUPPORT_REFECTION,
                        MONTANT_ARTICLE,
                        NM_ENVOI,
                        LB_ENVOI,
                        QTE_EXPEDIEE,
                        ID_LIGNCOMM,
                        NM_PARU,
                        DT_PLAN
                    ) 
                    VALUES (
                        avis_reinst_per.id_event, 
                        avis_reinst_per.type_offre, 
                        avis_reinst_per.sys_vente, 
                        avis_reinst_per.dt_fab, 
                        avis_reinst_per.dt_acquis, 
                        avis_reinst_per.dt_comm, 
                        avis_reinst_per.dt_event, 
                        avis_reinst_per.letchrono, 
                        avis_reinst_per.chrono_client, 
                        avis_reinst_per.insee_client, 
                        avis_reinst_per.pays_client, 
                        avis_reinst_per.v1_client, 
                        avis_reinst_per.v2_client, 
                        avis_reinst_per.v3_client, 
                        avis_reinst_per.v4_client, 
                        avis_reinst_per.v5_client, 
                        avis_reinst_per.cd_post_client, 
                        avis_reinst_per.ville_client, 
                        avis_reinst_per.email_client, 
                        avis_reinst_per.tel_client, 
                        avis_reinst_per.type_tiers, 
                        avis_reinst_per.lettiers, 
                        avis_reinst_per.chrono_tiers, 
                        avis_reinst_per.pays_tiers, 
                        avis_reinst_per.insee_tiers, 
                        avis_reinst_per.v1_tiers, 
                        avis_reinst_per.v2_tiers, 
                        avis_reinst_per.v3_tiers, 
                        avis_reinst_per.v4_tiers, 
                        avis_reinst_per.v5_tiers, 
                        avis_reinst_per.cd_post_tiers, 
                        avis_reinst_per.ville_tiers, 
                        avis_reinst_per.email_tiers, 
                        avis_reinst_per.tel_tiers, 
                        avis_reinst_per.dt_effet, 
                        avis_reinst_per.dt_rep, 
                        avis_reinst_per.cd_article, 
                        avis_reinst_per.type_prod, 
                        avis_reinst_per.cd_partenaire, 
                        avis_reinst_per.nm_comm, 
                        avis_reinst_per.nb_numero, 
                        avis_reinst_per.cd_mailing, 
                        avis_reinst_per.cd_statut, 
                        avis_reinst_per.dt_statut, 
                        avis_reinst_per.chrono_dest, 
                        avis_reinst_per.cd_produit, 
                        avis_reinst_per.chrono_edit, 
                        avis_reinst_per.chrono_full, 
                        avis_reinst_per.soc_vente, 
                        avis_reinst_per.dt_plan, 
                        avis_reinst_per.nm_ordre_type_event, 
                        avis_reinst_per.dt_exped_param, 
                        avis_reinst_per.chrono_presta, 
                        avis_reinst_per.dt_susp, 
                        avis_reinst_per.nm_disc, 
                        avis_reinst_per.nb_disc, 
                        avis_reinst_per.REFECTION, 
                        avis_reinst_per.id_event_lien, 
                        avis_reinst_per.cd_camp, 
                        avis_reinst_per.nm_abo, 
                        avis_reinst_per.type_liste, 
                        avis_reinst_per.tx_comm, 
                        avis_reinst_per.nature_article, 
                        avis_reinst_per.fg_exte, 
                        avis_reinst_per.type_annulation, 
                        avis_reinst_per.type_event, 
                        avis_reinst_per.supp_refection, 
                        avis_reinst_per.mt_article, 
                        avis_reinst_per.NM_ENVOI, 
                        avis_reinst_per.LB_ENVOI, 
                        avis_reinst_per.quant_exped, 
                        avis_reinst_per.id_ligne_comm, 
                        avis_reinst_per.nm_parution, 
                        avis_reinst_per.dt_planifier
                    );
                end;
                DELETE FROM SYSTEME.EVENEMENTS_CLIENTS_TMP event_client WHERE avis_reinst_per.id_event = event_client.ID_EVENEMENT; 
                deleted :=  deleted + sql%rowcount ;
                countloop := countloop + 1; 
            end loop;
            utl_file.put_line(log_file,dbtab||'Iterations: '||countloop);
             utl_file.put_line(log_file,dbtab||'Supprimmé: '|| deleted);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
            
        end p090_reinst_periode;
   
    procedure p100_rep_periode is
    /*
            Transferts des reprise période dans annulation_periode
    */
    countloop int := 0;
    typetier number(1);
    dt_rep date := null ;
    deleted int :=null ;
    cursor crsr_rep_per is
        SELECT
            rep_per_v.ID_EVENEMENT as id_event, 
            rep_per_v.TYPE_OFFRE as type_offre, 
            rep_per_v.SYSTEME_VENTE as sys_vente, 
            rep_per_v.DATEFABRICATION as dt_fab, 
            rep_per_v.DATEACQUISITION_FICHIER as dt_acquis, 
            rep_per_v.DATE_COMMANDE as dt_comm, 
            rep_per_v.DATE_EVENEMENT as dt_event, 
            rep_per_v.LETCHRONO as letchrono, 
            rep_per_v.CHRONOCLI as chrono_client, 
            rep_per_v.INSEECLIENT as insee_client, 
            rep_per_v.PAYSCLIENT as pays_client, 
            rep_per_v.V1CLIENT as v1_client, 
            rep_per_v.V2CLIENT as v2_client, 
            rep_per_v.V3CLIENT as v3_client, 
            rep_per_v.V4CLIENT as v4_client, 
            rep_per_v.V5CLIENT as v5_client, 
            rep_per_v.CPOSTCLIENT as cd_post_client, 
            rep_per_v.VILLECLIENT as ville_client, 
            rep_per_v.EMAILCLIENT as email_client, 
            rep_per_v.TELCLIENT as tel_client, 
            rep_per_v.TYPETIERS as type_tiers, 
            rep_per_v.LETTIERS as lettiers, 
            rep_per_v.CHRONOTIERS as chrono_tiers, 
            rep_per_v.PAYSTIERS as pays_tiers, 
            rep_per_v.INSEETIERS as insee_tiers, 
            rep_per_v.V1TIERS as v1_tiers, 
            rep_per_v.V2TIERS as v2_tiers, 
            rep_per_v.V3TIERS as v3_tiers, 
            rep_per_v.V4TIERS as v4_tiers, 
            rep_per_v.V5TIERS as v5_tiers, 
            rep_per_v.CPOSTTIERS as cd_post_tiers, 
            rep_per_v.VILLETIERS as ville_tiers, 
            rep_per_v.EMAILTIERS as email_tiers, 
            rep_per_v.TELTIERS as tel_tiers, 
            rep_per_v.DATEEFFET as dt_effet, 
            rep_per_v.DATEREPRISE as dt_rep, 
            rep_per_v.CODE_ARTICLE as cd_article, 
            rep_per_v.TYPE_PRODUIT as type_prod, 
            rep_per_v.CODEFPARTENAIRE as cd_partenaire, 
            rep_per_v.NUMCOMMANDE as nm_comm, 
            rep_per_v.NBNUM as nb_numero, 
            rep_per_v.CODEMAILING as cd_mailing, 
            rep_per_v.CODESTATUT as cd_statut, 
            rep_per_v.DATESTATUT as dt_statut, 
            rep_per_v.CHRONO_DEST as chrono_dest, 
            rep_per_v.CODE_PRODUIT as cd_produit, 
            rep_per_v.CHRONOEDITEUR as chrono_edit, 
            rep_per_v.CHRONOFULL as chrono_full, 
            rep_per_v.SOCIETE_VENTE as soc_vente, 
            rep_per_v.DATE_PLANNING as dt_plan, 
            rep_per_v.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
            rep_per_v.DATE_EXPEDITION_PARAMETRE as dt_exped_param, 
            rep_per_v.CHRONO_PRESTA as chrono_presta, 
            rep_per_v.DATE_SUSP_REP_ANN as dt_susp, 
            rep_per_v.NUM_DISQUETTE as nm_disc, 
            rep_per_v.NB_DISQUETTE as nb_disc, 
            rep_per_v.REFECTION, 
            rep_per_v.ID_EVENEMENT_LIEN as id_event_lien, 
            rep_per_v.CODE_CAMPAGNE as cd_camp, 
            rep_per_v.NUMERO_ABO as nm_abo, 
            rep_per_v.TYPE_LISTE as type_liste, 
            rep_per_v.TAUX_COMM as tx_comm, 
            rep_per_v.NATURE_ARTICLE as nature_article, 
            rep_per_v.FLAG_EXTENSION as fg_exte, 
            rep_per_v.TYPE_ANNULATION as type_annulation, 
            rep_per_v.TYPE_EVENEMENT as type_event, 
            rep_per_v.SUPPORT_REFECTION as supp_refection, 
            rep_per_v.MONTANT_ARTICLE as mt_article, 
            rep_per_v.NM_ENVOI, 
            rep_per_v.LB_ENVOI, 
            rep_per_v.QTE_EXPEDIEE as quant_exped, 
            rep_per_v.ID_LIGNCOMM as id_ligne_comm, 
            rep_per_v.NM_PARU as nm_parution, 
            rep_per_v.DT_PLAN as dt_planifier
        FROM SYSTEME.REPRISE_PERIODE_V rep_per_v;
    begin
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p120_rep_periode: récupération des avis de "systeme.reprise_periode_v" vers "systeme.annulation_periode"');
        for avis_reprise_per in crsr_rep_per
        loop
            begin
                INSERT INTO SYSTEME.ANNULATION_PERIODE (
                    ID_EVENEMENT,
                    TYPE_OFFRE,
                    SYSTEME_VENTE,
                    DATEFABRICATION,
                    DATEACQUISITION_FICHIER,
                    DATE_COMMANDE,
                    DATE_EVENEMENT,
                    LETCHRONO,
                    CHRONOCLI,	
                    INSEECLIENT,
                    PAYSCLIENT,
                    V1CLIENT,
                    V2CLIENT,
                    V3CLIENT,
                    V4CLIENT,
                    V5CLIENT,
                    CPOSTCLIENT,
                    VILLECLIENT,
                    EMAILCLIENT,
                    TELCLIENT,
                    TYPETIERS,
                    LETTIERS,
                    CHRONOTIERS,
                    PAYSTIERS,
                    INSEETIERS,
                    V1TIERS,
                    V2TIERS,
                    V3TIERS,
                    V4TIERS,
                    V5TIERS,
                    CPOSTTIERS,
                    VILLETIERS,
                    EMAILTIERS,
                    TELTIERS,
                    DATEEFFET,
                    DATEREPRISE,
                    CODE_ARTICLE,
                    TYPE_PRODUIT,
                    CODEFPARTENAIRE,
                    NUMCOMMANDE,
                    NBNUM,
                    CODEMAILING,
                    CODESTATUT,
                    DATESTATUT,
                    CHRONO_DEST,
                    CODE_PRODUIT,
                    CHRONOEDITEUR,
                    CHRONOFULL,
                    SOCIETE_VENTE,
                    DATE_PLANNING,
                    NUM_ORDRE_TYPE_EVENEMENT,
                    DATE_EXPEDITION_PARAMETRE,
                    CHRONO_PRESTA,
                    DATE_SUSP_REP_ANN,
                    NUM_DISQUETTE,
                    NB_DISQUETTE,
                    REFECTION,
                    ID_EVENEMENT_LIEN,
                    CODE_CAMPAGNE,
                    NUMERO_ABO,
                    TYPE_LISTE,
                    TAUX_COMM,
                    NATURE_ARTICLE,
                    FLAG_EXTENSION,
                    TYPE_ANNULATION,
                    TYPE_EVENEMENT,
                    SUPPORT_REFECTION,
                    MONTANT_ARTICLE,
                    NM_ENVOI,
                    LB_ENVOI,
                    QTE_EXPEDIEE,
                    ID_LIGNCOMM,
                    NM_PARU,
                    DT_PLAN
                ) 
                VALUES (
                    avis_reprise_per.id_event, 
                    avis_reprise_per.type_offre, 
                    avis_reprise_per.sys_vente, 
                    avis_reprise_per.dt_fab, 
                    avis_reprise_per.dt_acquis, 
                    avis_reprise_per.dt_comm, 
                    avis_reprise_per.dt_event, 
                    avis_reprise_per.letchrono, 
                    avis_reprise_per.chrono_client, 
                    avis_reprise_per.insee_client, 
                    avis_reprise_per.pays_client, 
                    avis_reprise_per.v1_client, 
                    avis_reprise_per.v2_client, 
                    avis_reprise_per.v3_client, 
                    avis_reprise_per.v4_client, 
                    avis_reprise_per.v5_client, 
                    avis_reprise_per.cd_post_client, 
                    avis_reprise_per.ville_client, 
                    avis_reprise_per.email_client, 
                    avis_reprise_per.tel_client, 
                    avis_reprise_per.type_tiers, 
                    avis_reprise_per.lettiers, 
                    avis_reprise_per.chrono_tiers, 
                    avis_reprise_per.pays_tiers, 
                    avis_reprise_per.insee_tiers, 
                    avis_reprise_per.v1_tiers, 
                    avis_reprise_per.v2_tiers, 
                    avis_reprise_per.v3_tiers, 
                    avis_reprise_per.v4_tiers, 
                    avis_reprise_per.v5_tiers, 
                    avis_reprise_per.cd_post_tiers, 
                    avis_reprise_per.ville_tiers, 
                    avis_reprise_per.email_tiers, 
                    avis_reprise_per.tel_tiers, 
                    avis_reprise_per.dt_effet, 
                    avis_reprise_per.dt_rep, 
                    avis_reprise_per.cd_article, 
                    avis_reprise_per.type_prod, 
                    avis_reprise_per.cd_partenaire, 
                    avis_reprise_per.nm_comm, 
                    avis_reprise_per.nb_numero, 
                    avis_reprise_per.cd_mailing, 
                    avis_reprise_per.cd_statut, 
                    avis_reprise_per.dt_statut, 
                    avis_reprise_per.chrono_dest, 
                    avis_reprise_per.cd_produit, 
                    avis_reprise_per.chrono_edit, 
                    avis_reprise_per.chrono_full, 
                    avis_reprise_per.soc_vente, 
                    avis_reprise_per.dt_plan, 
                    avis_reprise_per.nm_ordre_type_event, 
                    avis_reprise_per.dt_exped_param, 
                    avis_reprise_per.chrono_presta, 
                    avis_reprise_per.dt_susp, 
                    avis_reprise_per.nm_disc, 
                    avis_reprise_per.nb_disc, 
                    avis_reprise_per.REFECTION, 
                    avis_reprise_per.id_event_lien, 
                    avis_reprise_per.cd_camp, 
                    avis_reprise_per.nm_abo, 
                    avis_reprise_per.type_liste, 
                    avis_reprise_per.tx_comm, 
                    avis_reprise_per.nature_article, 
                    avis_reprise_per.fg_exte, 
                    avis_reprise_per.type_annulation, 
                    avis_reprise_per.type_event, 
                    avis_reprise_per.supp_refection, 
                    avis_reprise_per.mt_article, 
                    avis_reprise_per.NM_ENVOI, 
                    avis_reprise_per.LB_ENVOI, 
                    avis_reprise_per.quant_exped, 
                    avis_reprise_per.id_ligne_comm, 
                    avis_reprise_per.nm_parution, 
                    avis_reprise_per.dt_planifier
                );
            end;
            DELETE FROM SYSTEME.EVENEMENTS_CLIENTS_TMP event_client WHERE avis_reprise_per.id_event = event_client.ID_EVENEMENT; 
            deleted := deleted + sql%rowcount ;
            countloop := countloop + 1; 
        end loop;
        utl_file.put_line(log_file,dbtab||'Iterations: '||countloop);
         utl_file.put_line(log_file,dbtab||'Supprimmé: '|| deleted);
        utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
    end p100_rep_periode;
        
    procedure p110_produit is
        /*
            Mise à jour des informations relatives au produit
        */
        countloop int := 0 ;
        cursor crsr_updprod is
        SELECT
            produit.CODE_PRODUIT as cd_produit, 
            produit.CODE_CHRONO_PRESTATAIRE1 as cd_chrono_presta1, 
            produit.CODE_CHRONO_FULFILLMENT as cd_chrono_full, 
            produit.CODE_CHRONO_COMM1_EDITEUR as cd_chrono_comm1_editeur, 
            produit.TYPE_PRODUIT as type_produit
        FROM SYSTEME.PRODUIT produit;

        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p130_produit: Mise à jour des produits de "systeme.produit" vers "systeme.evenements_clients_tmp"');
            for avis_updprod in crsr_updprod
            loop
                UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client SET 
                    event_client.TYPE_PRODUIT = avis_updprod.type_produit,
                    event_client.CHRONOEDITEUR = avis_updprod.cd_chrono_comm1_editeur,
                    event_client.CHRONOFULL = avis_updprod.cd_chrono_full,
                    event_client.CHRONO_PRESTA = avis_updprod.cd_chrono_presta1 
                WHERE event_client.CODE_PRODUIT = avis_updprod.cd_produit ;
                if sql%rowcount > 0 then
                    countloop := countloop + sql%rowcount;
                end if;
            end loop;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '|| countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p110_produit;

    procedure p120_type_event is
        /*
            Mise à jour des informations relatives au type d'evenement
        */
        countloop int := 0 ;
        cursor crsr_updtype is
            SELECT 
                type_event.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
                type_event.TYPE_EVENEMENT as type_event, 
                type_event.TYPE_OFFRE as type_offre
            FROM SYSTEME.TYPE_EVENEMENTS type_event;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p140_type_event: Mise à jour des types d évenements de "systeme.type_evenements" vers "systeme.evenements_clients_tmp"');
            for avis_updtype in crsr_updtype
            loop
                UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client SET
                    event_client.NUM_ORDRE_TYPE_EVENEMENT = avis_updtype.nm_ordre_type_event
                WHERE event_client.TYPE_EVENEMENT = avis_updtype.type_event AND event_client.TYPE_OFFRE = avis_updtype.type_offre ;
                if sql%rowcount > 0 then
                    countloop := countloop + sql%rowcount;
                end if;
            end loop;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '|| countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p120_type_event;

    procedure p130_date_planning is
        /*
            Mise à jour des informations relatives aux dates du planning et aux dates d'expédition
        */
        countloop int := 0 ;
        cursor crsr_upddate is
            SELECT 
                date_plan.CODE_PRODUIT as cd_produit, 
                date_plan.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
                date_plan.DATE_PLANNING as dt_planning
            FROM SYSTEME.DATE_PLANNING date_plan;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p150_date_planning: Mise à jour des dates du planning de "systeme.date_planning" vers "systeme.evenements_clients_tmp"');
            if dt_traitement = boa_admin.dernier_jour_ouvre(dt_traitement) then
                utl_file.put_line(log_file,dbtab||'DERNIER JOUR OUVRE DU MOIS: FORCAGE DES EXPEDITIONS');
                update systeme.evenements_clients_tmp set
                    date_expedition_parametre = dt_traitement,
                    date_planning = dt_traitement ;
            else
                utl_file.put_line(log_file,dbtab||'DATE DIFFÉRENTE DU DERNIER JOUR OUVRÉ DU MOIS: TRAITEMENT NORMAL');
                for avis_upddate in crsr_upddate
                loop
                    UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client SET
                        event_client.DATE_PLANNING = avis_upddate.dt_planning,
                        event_client.DATE_EXPEDITION_PARAMETRE = avis_upddate.dt_planning 
                    WHERE event_client.CODE_PRODUIT = avis_upddate.cd_produit AND event_client.NUM_ORDRE_TYPE_EVENEMENT = avis_upddate.nm_ordre_type_event ;
                     if sql%rowcount > 0 then
                        countloop := countloop + sql%rowcount;
                    end if;
                end loop;
                utl_file.put_line(log_file,dbtab||'Mise à jour: '|| countloop);
            end if;
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p130_date_planning ;

    procedure p140_chrono_dest is
        /*
            Mise à jour des informations relatives au chrono destinataire
        */
        countloop int := 0 ;
        cursor crsr_updchronodest is
            SELECT 
                chrn_dst.CODE_PRODUIT as cd_produit, 
                chrn_dst.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
                chrn_dst.CHRONO_DESTINATAIRE as  chrono_dest      
            FROM SYSTEME.CHRONO_DESTINATAIRE chrn_dst;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p160_chrono_dest: Mise à jour des dates des chrono destinataire de "systeme.chrono_destinataire" vers "systeme.evenements_clients_tmp"');  
            for avis_updchronodest in crsr_updchronodest
            loop
                UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client SET
                    event_client.CHRONO_DEST = avis_updchronodest.chrono_dest 
                WHERE event_client.CODE_PRODUIT = avis_updchronodest.cd_produit AND event_client.NUM_ORDRE_TYPE_EVENEMENT = avis_updchronodest.nm_ordre_type_event ;
                if sql%rowcount > 0 then
                    countloop := countloop + sql%rowcount;
                end if;
            end loop;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '|| countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p140_chrono_dest;

    procedure p160_updchrono_dest is
        /*
            Mise à jour de certain chrono ~
        */
        countloop int := 0 ;
        cursor crsr_updchrdest is
            SELECT 
                offre.CODE_CAMPAGNE as cd_camp,
                offre.CODE_ARTICLE as cd_article 
            FROM SYSTEME.OFFRE offre 
            WHERE NOT (offre.CODE_ARTICLE_EXTENSION IS NULL );
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p180_updchrono_dest:Mise à jour des offres et chrono');
            for avis_updchrdest in crsr_updchrdest
            loop
                UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client SET event_client.CHRONO_DEST = '08013000' 
                WHERE event_client.CHRONO_DEST = '08013001' 
                    AND event_client.CODE_CAMPAGNE = avis_updchrdest.cd_camp 
                    AND event_client.CODE_ARTICLE = avis_updchrdest.cd_article 
                    AND event_client.TYPE_EVENEMENT IN ('ANN','AN3');
                if sql%rowcount > 0 then
                    countloop := countloop + sql%rowcount;
                end if;
            end loop;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p160_updchrono_dest;

    procedure p170_remise_status is
        /*
            remise des status ~
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> Remise des statuts: ');
            
            utl_file.put_line(log_file,dbtab||'Requête 1:');
            UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client_tmp SET 
                event_client_tmp.DATE_EXPEDITION_PARAMETRE = NULL 
            WHERE event_client_tmp.CHRONOEDITEUR IS NULL;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            
            utl_file.put_line(log_file,dbtab||'requête 2:');
            UPDATE SYSTEME.EVENEMENTS_CLIENTS_TMP event_client_tmp SET
                event_client_tmp.SOCIETE_VENTE = 3 
            WHERE SUBSTR(event_client_tmp.CHRONOCLI,1,1)  = '6';
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p170_remise_status;

    procedure p180_generation_mes is
        countloop int := 0;
        typetier number(1);
        dt_rep date := null;
        nm_ordre_type_event int := null;
        /*
            Génération des mise en service (traitement de la vue)
        */
        cursor crsr_gen_mes is
            SELECT
                mes.TYPE_OFFRE as type_offre, 
                mes.SYSTEME_VENTE as systeme_vente, 
                mes.DATEFABRICATION as dt_fab, 
                mes.DATEACQUISITION_FICHIER as dt_acqu, 
                mes.DATE_COMMANDE as dt_comm, 
                mes.DATE_EVENEMENT as dt_event, 
                mes.LETCHRONO as letchrono, 
                mes.CHRONOCLI as chrono_client, 
                mes.INSEECLIENT as insee_client, 
                mes.PAYSCLIENT as pays_client, 
                mes.V1CLIENT as v1_client, 
                mes.V2CLIENT as v2_client, 
                mes.V3CLIENT as v3_client,
                mes.V4CLIENT as v4_client, 
                mes.V5CLIENT as v5_client, 
                mes.CPOSTCLIENT as cd_post_client, 
                mes.VILLECLIENT as ville_client, 
                mes.EMAILCLIENT as email_client, 
                mes.TELCLIENT as tel_client, 
                mes.TYPETIERS as type_tiers, 
                mes.LETTIERS as lettiers, 
                mes.CHRONOTIERS as chrono_tiers, 
                mes.PAYSTIERS as pays_tiers, 
                mes.INSEETIERS as insee_tiers, 
                mes.V1TIERS as v1_tiers, 
                mes.V2TIERS as v2_tiers, 
                mes.V3TIERS as v3_tiers, 
                mes.V4TIERS as v4_tiers, 
                mes.V5TIERS as v5_tiers, 
                mes.CPOSTTIERS as cd_post_tiers, 
                mes.VILLETIERS as ville_tiers, 
                mes.EMAILTIERS as email_tiers, 
                mes.TELTIERS as tel_tiers, 
                mes.DATEEFFET as dt_effet, 
                mes.DATEREPRISE as dt_reprise, 
                mes.CODE_ARTICLE as cd_article, 
                mes.TYPE_PRODUIT as type_produit, 
                mes.CODEFPARTENAIRE as cd_partenaire, 
                mes.NUMCOMMANDE as nm_comm, 
                mes.NBNUM as nb_numero, 
                mes.CODEMAILING as cd_mail, 
                mes.CODESTATUT as cd_statut, 
                mes.DATESTATUT as dt_statut, 
                mes.CHRONO_DEST as chrono_dest, 
                mes.CODE_PRODUIT as cd_produit, 
                mes.CHRONOEDITEUR as chrono_editeur, 
                mes.CHRONOFULL as chrono_full, 
                mes.SOCIETE_VENTE as societe_vente, 
                mes.DATE_PLANNING as dt_planning, 
                mes.DATE_EXPEDITION_PARAMETRE as dt_exped_param, 
                mes.CHRONO_PRESTA as chrono_presta, 
                mes.DATE_SUSP_REP_ANN as dt_susp_rep_ann, 
                mes.NUM_DISQUETTE as nm_disc, 
                mes.NB_DISQUETTE as nb_disc, 
                mes.REFECTION as refection, 
                mes.ID_EVENEMENT_LIEN as id_event_lien, 
                mes.CODE_CAMPAGNE as cd_camp, 
                mes.NUMERO_ABO as nm_abo, 
                mes.TYPE_LISTE as type_liste, 
                mes.TAUX_COMM as tx_comm,  
                mes.NATURE_ARTICLE as nature_article, 
                mes.FLAG_EXTENSION as fg_exte, 
                mes.TYPE_ANNULATION as type_annulation, 
                mes.MONTANT_ARTICLE as mt_article, 
                mes.NM_ENVOI as nm_envoi, 
                mes.LB_ENVOI as lb_envoi, 
                mes.QTE_EXPEDIEE as quant_exped, 
                mes.ID_LIGNCOMM as id_ligne_comm,
                mes.NM_PARU as nm_parution, 
                mes.DT_PLAN as dt_plan, 
                mes.ID_COMMWEB as id_commweb, 
                mes.LB_REFEEXTECDE as lb_ref_exte_code 
            FROM SYSTEME.MISE_EN_SERVICE_VIEW mes;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p200_generation_mes: Récupération des mise en service de "systeme.mise_en_service_view" vers "systeme.evenements_clients_tmp"');
            for avis_gen_mes in crsr_gen_mes
            loop
                begin
                    SELECT NUM_ORDRE_TYPE_EVENEMENT into nm_ordre_type_event 
                    FROM SYSTEME.TYPE_EVENEMENTS
                    where type_evenement = 'MES' 
                    and TYPE_OFFRE = avis_gen_mes.type_offre;

                    INSERT INTO SYSTEME.EVENEMENTS_CLIENTS_TMP (
                        ID_EVENEMENT,
                        TYPE_OFFRE,
                        SYSTEME_VENTE,
                        DATEFABRICATION,
                        DATEACQUISITION_FICHIER,
                        DATE_COMMANDE,
                        DATE_EVENEMENT,
                        LETCHRONO,
                        CHRONOCLI,
                        INSEECLIENT,
                        PAYSCLIENT,
                        V1CLIENT,
                        V2CLIENT,
                        V3CLIENT,
                        V4CLIENT,
                        CPOSTCLIENT,
                        VILLECLIENT,
                        EMAILCLIENT,
                        TELCLIENT,
                        TYPETIERS,
                        LETTIERS,
                        CHRONOTIERS,
                        PAYSTIERS,
                        INSEETIERS,
                        V1TIERS,
                        V2TIERS,
                        V3TIERS,
                        V4TIERS,
                        CPOSTTIERS,
                        VILLETIERS,
                        EMAILTIERS,
                        TELTIERS,
                        DATEEFFET,
                        DATEREPRISE,
                        CODE_ARTICLE,
                        TYPE_PRODUIT,
                        CODEFPARTENAIRE,
                        NUMCOMMANDE,
                        NBNUM,
                        CODEMAILING,
                        CODESTATUT,
                        DATESTATUT,
                        CHRONO_DEST,
                        CODE_PRODUIT,
                        CHRONOEDITEUR,
                        CHRONOFULL,
                        SOCIETE_VENTE,
                        DATE_PLANNING,
                        NUM_ORDRE_TYPE_EVENEMENT,
                        DATE_EXPEDITION_PARAMETRE,
                        CHRONO_PRESTA,
                        DATE_SUSP_REP_ANN,
                        NUM_DISQUETTE,
                        NB_DISQUETTE,
                        REFECTION,
                        ID_EVENEMENT_LIEN,
                        CODE_CAMPAGNE,
                        NUMERO_ABO,
                        TYPE_LISTE,
                        TAUX_COMM,
                        NATURE_ARTICLE,
                        FLAG_EXTENSION,
                        TYPE_ANNULATION,
                        TYPE_EVENEMENT,
                        SUPPORT_REFECTION,
                        MONTANT_ARTICLE,
                        V5CLIENT,
                        V5TIERS,
                        NM_ENVOI,
                        LB_ENVOI,
                        QTE_EXPEDIEE,
                        ID_LIGNCOMM,
                        NM_PARU,
                        DT_PLAN,
                        ID_COMMWEB,
                        LB_REFEEXTECDE
                    ) 
                    VALUES (
                        systeme.seq_evenement_client.nextval,
                        avis_gen_mes.TYPE_OFFRE,
                        avis_gen_mes.SYSTEME_VENTE,
                        avis_gen_mes.DT_FAB,
                        avis_gen_mes.DT_ACQU,
                        avis_gen_mes.DT_COMM,
                        avis_gen_mes.DT_EVENT,
                        avis_gen_mes.LETCHRONO,
                        avis_gen_mes.CHRONO_CLIENT,
                        avis_gen_mes.INSEE_CLIENT,
                        avis_gen_mes.PAYS_CLIENT,
                        avis_gen_mes.V1_CLIENT,
                        avis_gen_mes.V2_CLIENT,
                        avis_gen_mes.V3_CLIENT,
                        avis_gen_mes.V4_CLIENT,
                        avis_gen_mes.CD_POST_CLIENT,
                        avis_gen_mes.VILLE_CLIENT,
                        avis_gen_mes.EMAIL_CLIENT,
                        avis_gen_mes.TEL_CLIENT,
                        avis_gen_mes.TYPE_TIERS,
                        avis_gen_mes.LETTIERS,
                        avis_gen_mes.CHRONO_TIERS,
                        avis_gen_mes.PAYS_TIERS,
                        avis_gen_mes.INSEE_TIERS,
                        avis_gen_mes.V1_TIERS,
                        avis_gen_mes.V2_TIERS,
                        avis_gen_mes.V3_TIERS, 
                        avis_gen_mes.V4_TIERS,
                        avis_gen_mes.CD_POST_TIERS,
                        avis_gen_mes.VILLE_TIERS,
                        avis_gen_mes.EMAIL_TIERS,
                        avis_gen_mes.TEL_TIERS,
                        avis_gen_mes.DT_EFFET,
                        avis_gen_mes.DT_REPRISE,
                        avis_gen_mes.CD_ARTICLE,
                        avis_gen_mes.TYPE_PRODUIT,
                        avis_gen_mes.CD_PARTENAIRE,
                        avis_gen_mes.NM_COMM,
                        avis_gen_mes.NB_NUMERO,
                        avis_gen_mes.CD_MAIL,
                        avis_gen_mes.CD_STATUT,
                        avis_gen_mes.DT_STATUT,
                        avis_gen_mes.CHRONO_FULL,
                        avis_gen_mes.CD_PRODUIT,
                        avis_gen_mes.CHRONO_EDITEUR,
                        avis_gen_mes.CHRONO_FULL,
                        avis_gen_mes.SOCIETE_VENTE,
                        avis_gen_mes.DT_PLANNING,
                        NM_ORDRE_TYPE_EVENT,
                        avis_gen_mes.DT_EXPED_PARAM,
                        avis_gen_mes.CHRONO_PRESTA,
                        avis_gen_mes.DT_SUSP_REP_ANN,
                        avis_gen_mes.NM_DISC,
                        avis_gen_mes.NB_DISC,
                        avis_gen_mes.REFECTION,
                        avis_gen_mes.ID_EVENT_LIEN,
                        avis_gen_mes.CD_CAMP,
                        avis_gen_mes.NM_ABO,
                        avis_gen_mes.TYPE_LISTE,
                        avis_gen_mes.TX_COMM,
                        avis_gen_mes.NATURE_ARTICLE,
                        avis_gen_mes.FG_EXTE,
                        avis_gen_mes.TYPE_ANNULATION,
                        'MES',
                        null,
                        avis_gen_mes.MT_ARTICLE,
                        null,
                        null,
                        avis_gen_mes.NM_ENVOI,
                        avis_gen_mes.LB_ENVOI,
                        avis_gen_mes.QUANT_EXPED,
                        avis_gen_mes.ID_LIGNE_COMM,
                        avis_gen_mes.NM_PARUTION,
                        avis_gen_mes.DT_PLAN,
                        avis_gen_mes.ID_COMMWEB,
                        avis_gen_mes.LB_REF_EXTE_CODE
                    );
                end;
                countloop := countloop + 1;
            end loop;
            utl_file.put_line(log_file,dbtab||'Itérations: '||countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p180_generation_mes ;

    procedure p190_offre is
        /*
            Mise à jour des informations relatives aux offres
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p210_offre: Mise à jour des offres');
            update systeme.evenements_clients_tmp ec set ec.SOCIETE_VENTE = '2' where ec.SOCIETE_VENTE = '3' and exists (select 1 from systeme.offre o where o.FG_OFUP = 2 and o.CODE_CAMPAGNE = ec.CODE_CAMPAGNE and to_char(o.CODE_ARTICLE) = ec.CODE_ARTICLE);
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p190_offre;

    procedure p200_supp_code_statut0 is
        /*
            suppression dans evenements_clients des avis non expédié précédemment extrait
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p220_supp_code_statut0:');
            DELETE FROM SYSTEME.EVENEMENTS_CLIENTS event_client WHERE event_client.CODESTATUT = 0 ;
            utl_file.put_line(log_file,dbtab||'Lignes supprimées: '||sql%rowcount);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p200_supp_code_statut0;

    procedure p210_tmp2event_client is
        countloop int := 0;
        typetier number(1);
        dt_rep date := null;
        /*
            Transfert des avis de la table tampon à evenements_clients
        */
        cursor crsr_event_client_tmp is
            SELECT
                event_client.ID_EVENEMENT as id_event, 
                event_client.TYPE_OFFRE, 
                event_client.SYSTEME_VENTE, 
                event_client.DATEFABRICATION as dt_fab, 
                event_client.DATEACQUISITION_FICHIER as dt_acqu, 
                event_client.DATE_COMMANDE as dt_comm, 
                event_client.DATE_EVENEMENT as dt_event, 
                event_client.LETCHRONO, 
                event_client.CHRONOCLI as chrono_client, 
                event_client.INSEECLIENT as insee_client, 
                event_client.PAYSCLIENT as pays_client, 
                event_client.V1CLIENT as v1_client, 
                event_client.V2CLIENT as v2_client, 
                event_client.V3CLIENT as v3_client, 
                event_client.V4CLIENT as v4_client, 
                event_client.CPOSTCLIENT as cd_post_client, 
                event_client.VILLECLIENT as ville_client, 
                event_client.EMAILCLIENT as email_client, 
                event_client.TELCLIENT as tel_client, 
                event_client.TYPETIERS as type_tiers, 
                event_client.LETTIERS, 
                event_client.CHRONOTIERS as chrono_tiers, 
                event_client.PAYSTIERS as pays_tiers, 
                event_client.INSEETIERS as insee_tiers, 
                event_client.V1TIERS as v1_tiers, 
                event_client.V2TIERS as v2_tiers, 
                event_client.V3TIERS as v3_tiers, 
                event_client.V4TIERS as v4_tiers, 
                event_client.CPOSTTIERS as cd_post_tiers, 
                event_client.VILLETIERS as ville_tiers, 
                event_client.EMAILTIERS as email_tiers, 
                event_client.TELTIERS as tel_tiers, 
                event_client.DATEEFFET as dt_effet, 
                event_client.DATEREPRISE as dt_reprise, 
                event_client.CODE_ARTICLE as cd_article, 
                event_client.TYPE_PRODUIT as type_produit, 
                event_client.CODEFPARTENAIRE as cd_partenaire, 
                event_client.NUMCOMMANDE as nm_comm, 
                event_client.NBNUM as nb_numero, 
                event_client.CODEMAILING as cd_mailing, 
                event_client.CODESTATUT as cd_statut, 
                event_client.DATESTATUT as dt_statut, 
                event_client.CHRONO_DEST as chrono_dest, 
                event_client.CODE_PRODUIT as cd_produit, 
                event_client.CHRONOEDITEUR as chrono_editeur, 
                event_client.CHRONOFULL as chrono_full, 
                event_client.SOCIETE_VENTE as societe_vente, 
                event_client.DATE_PLANNING as dt_planning, 
                event_client.NUM_ORDRE_TYPE_EVENEMENT as nm_ordre_type_event, 
                event_client.DATE_EXPEDITION_PARAMETRE as dt_exped_param, 
                event_client.CHRONO_PRESTA as chrono_presta, 
                event_client.DATE_SUSP_REP_ANN as dt_susp, 
                event_client.NUM_DISQUETTE as nm_disc, 
                event_client.NB_DISQUETTE as nb_disc, 
                event_client.REFECTION as refection, 
                event_client.ID_EVENEMENT_LIEN id_event_lien, 
                event_client.CODE_CAMPAGNE as cd_camp, 
                event_client.NUMERO_ABO as nm_abo, 
                event_client.TYPE_LISTE as type_liste, 
                event_client.TAUX_COMM as tx_comm, 
                event_client.NATURE_ARTICLE, 
                event_client.FLAG_EXTENSION as fg_exte, 
                event_client.TYPE_ANNULATION, 
                event_client.TYPE_EVENEMENT as type_event, 
                event_client.SUPPORT_REFECTION as supp_refection, 
                event_client.MONTANT_ARTICLE as mt_article, 
                event_client.V5CLIENT as v5_client, 
                event_client.V5TIERS as v5_tiers, 
                event_client.NM_ENVOI, 
                event_client.LB_ENVOI, 
                event_client.QTE_EXPEDIEE as quant_exped, 
                event_client.ID_LIGNCOMM as id_ligne_comm, 
                event_client.NM_PARU as nm_parution, 
                event_client.DT_PLAN,
                event_client.ID_COMMWEB, 
                event_client.LB_REFEEXTECDE as lb_ref_exte_code 
            FROM SYSTEME.EVENEMENTS_CLIENTS_TMP event_client;
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p230_tmp2event_client: Insertion de "systeme.evenements_clients_tmp" dans "systeme.evenements_clients"');
            utl_file.put_line(log_file,dbtab||'Avis placé en codestatut 1:');
            update systeme.evenements_clients_tmp set codestatut = 1 where 
                type_offre is not null 
                and type_offre <> '???' 
                and type_produit is not null
                and date_expedition_parametre is not null
                and chrono_dest is not null 
                and date_expedition_parametre = dt_traitement ;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '|| sql%rowcount);
            for tmp in crsr_event_client_tmp
            loop
                begin
                    INSERT INTO SYSTEME.EVENEMENTS_CLIENTS (
                        ID_EVENEMENT,
                        TYPE_OFFRE,
                        SYSTEME_VENTE,
                        DATEFABRICATION,
                        DATEACQUISITION_FICHIER,
                        DATE_COMMANDE,
                        DATE_EVENEMENT,
                        LETCHRONO,
                        CHRONOCLI,
                        INSEECLIENT,
                        PAYSCLIENT,
                        V1CLIENT,
                        V2CLIENT,
                        V3CLIENT,
                        V4CLIENT,
                        CPOSTCLIENT,
                        VILLECLIENT,
                        EMAILCLIENT,
                        TELCLIENT,
                        TYPETIERS,
                        LETTIERS,
                        CHRONOTIERS,
                        PAYSTIERS,
                        INSEETIERS,
                        V1TIERS,
                        V2TIERS,
                        V3TIERS,
                        V4TIERS,
                        CPOSTTIERS,
                        VILLETIERS,
                        EMAILTIERS,
                        TELTIERS,
                        DATEEFFET,
                        DATEREPRISE,
                        CODE_ARTICLE,
                        TYPE_PRODUIT,
                        CODEFPARTENAIRE,
                        NUMCOMMANDE,
                        NBNUM,
                        CODEMAILING,
                        CODESTATUT,
                        DATESTATUT,
                        CHRONO_DEST,
                        CODE_PRODUIT,
                        CHRONOEDITEUR,
                        CHRONOFULL,
                        SOCIETE_VENTE,
                        DATE_PLANNING,
                        NUM_ORDRE_TYPE_EVENEMENT,
                        DATE_EXPEDITION_PARAMETRE,
                        CHRONO_PRESTA,
                        DATE_SUSP_REP_ANN,
                        NUM_DISQUETTE,
                        NB_DISQUETTE,
                        REFECTION,
                        ID_EVENEMENT_LIEN,
                        CODE_CAMPAGNE,
                        NUMERO_ABO,
                        TYPE_LISTE,
                        TAUX_COMM,
                        NATURE_ARTICLE,
                        FLAG_EXTENSION,
                        TYPE_ANNULATION,
                        TYPE_EVENEMENT,
                        SUPPORT_REFECTION,
                        MONTANT_ARTICLE,
                        V5CLIENT,
                        V5TIERS,
                        NM_ENVOI,
                        LB_ENVOI,
                        QTE_EXPEDIEE,
                        ID_LIGNCOMM,
                        NM_PARU,
                        DT_PLAN,
                        ID_COMMWEB,
                        LB_REFEEXTECDE
                    )
                    VALUES (
                        tmp.ID_EVENT,
                        tmp.TYPE_OFFRE,
                        tmp.SYSTEME_VENTE,
                        tmp.DT_FAB,
                        tmp.DT_ACQU,
                        tmp.DT_COMM,
                        tmp.DT_EVENT,
                        tmp.LETCHRONO,
                        tmp.CHRONO_CLIENT,
                        tmp.INSEE_CLIENT,
                        tmp.PAYS_CLIENT,
                        tmp.V1_CLIENT,
                        tmp.V2_CLIENT,
                        tmp.V3_CLIENT,
                        tmp.V4_CLIENT,
                        tmp.CD_POST_CLIENT,
                        tmp.VILLE_CLIENT,
                        tmp.EMAIL_CLIENT,
                        tmp.TEL_CLIENT,
                        tmp.TYPE_TIERS,
                        tmp.LETTIERS,
                        tmp.CHRONO_TIERS,
                        tmp.PAYS_TIERS,
                        tmp.INSEE_TIERS,
                        tmp.V1_TIERS,
                        tmp.V2_TIERS,
                        tmp.V3_TIERS,
                        tmp.V4_TIERS,
                        tmp.CD_POST_TIERS,
                        tmp.VILLE_TIERS,
                        tmp.EMAIL_TIERS,
                        tmp.TEL_TIERS,
                        tmp.DT_EFFET,
                        tmp.DT_REPRISE,
                        tmp.CD_ARTICLE,
                        tmp.TYPE_PRODUIT,
                        tmp.CD_PARTENAIRE,
                        tmp.NM_COMM,
                        tmp.NB_NUMERO,
                        tmp.CD_MAILING,
                        tmp.CD_STATUT,
                        tmp.DT_STATUT,
                        tmp.CHRONO_DEST,
                        tmp.CD_PRODUIT,
                        tmp.CHRONO_EDITEUR,
                        tmp.CHRONO_FULL,
                        tmp.SOCIETE_VENTE,
                        tmp.DT_PLANNING,
                        tmp.NM_ORDRE_TYPE_EVENT,
                        tmp.DT_EXPED_PARAM,
                        tmp.CHRONO_PRESTA,
                        tmp.DT_SUSP,
                        tmp.NM_DISC,
                        tmp.NB_DISC,
                        tmp.REFECTION,
                        tmp.ID_EVENT_LIEN,
                        tmp.CD_CAMP,
                        tmp.NM_ABO,
                        tmp.TYPE_LISTE,
                        tmp.TX_COMM,
                        tmp.NATURE_ARTICLE,
                        tmp.FG_EXTE,
                        tmp.TYPE_ANNULATION,
                        tmp.TYPE_EVENT,
                        tmp.SUPP_REFECTION,
                        tmp.MT_ARTICLE,
                        tmp.V5_CLIENT,
                        tmp.V5_TIERS,
                        tmp.NM_ENVOI,
                        tmp.LB_ENVOI,
                        tmp.QUANT_EXPED,
                        tmp.ID_LIGNE_COMM,
                        tmp.NM_PARUTION,
                        tmp.DT_PLAN,
                        tmp.ID_COMMWEB,
                        tmp.LB_REF_EXTE_CODE
                    );
                end;
                countloop := countloop + 1;
            end loop;
            utl_file.put_line(log_file,dbtab||'Itérations: '||countloop);
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p210_tmp2event_client ;

    procedure p220_intercept_num is
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p240_intercept_num: Interception des avis numériques');
            utl_file.put_line(log_file,dbtab||'Requête 1:');
            update systeme.evenements_clients ec  set ec.CODESTATUT = 0 where ec.CODE_CAMPAGNE = 93 and ec.CODE_ARTICLE = '23253x' and ec.CODESTATUT >= 0;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount||rtrn);
            
            utl_file.put_line(log_file,dbtab||'Requête 2:');
            update systeme.evenements_clients t set t.CHRONO_DEST = '08058002'
            where  t.CHRONO_DEST = '08058001'  and t.TYPE_EVENEMENT in ('ANN','AN3') and t.CODESTATUT > 0 ;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount||rtrn);

            utl_file.put_line(log_file,dbtab||'Requête 3:');
            update systeme.evenements_clients t set t.CHRONO_DEST = '08058003'
            where  t.CHRONO_DEST = '08058000'  and t.TYPE_EVENEMENT in ('ANN','AN3') and t.CODESTATUT > 0 ;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount||rtrn);

            utl_file.put_line(log_file,dbtab||'Requête 4:');
            update systeme.evenements_clients t set t.CHRONO_DEST = '08062001'
            where  t.CHRONO_DEST = '08062000'  and t.TYPE_EVENEMENT like 'CH%' and t.CODESTATUT > 0 and t.CODE_PRODUIT <> 2674 ;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount||rtrn);

            utl_file.put_line(log_file,dbtab||'Requête 5:');
            update systeme.evenements_clients t set t.CHRONO_DEST = '08062001'
            where  t.CHRONO_DEST = '08062002'  and t.TYPE_EVENEMENT like 'CH%' and t.CODESTATUT > 0 and t.CODE_PRODUIT = 2674 ;
            utl_file.put_line(log_file,dbtab||'Mise à jour: '||sql%rowcount||rtrn);
            
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p220_intercept_num ;
        
    procedure p230_maj_quantieme is
        /*
            Mise à jour du prochain jour de traitement
        */
        begin
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'> p250_maj_quantieme: Mise à jour du prochain quantieme');
            if to_char(dt_traitement, 'D') = 5 then
                dt_traitement := dt_traitement + 3;
            else
                dt_traitement := dt_traitement + 1;
            end if;
            
            while systeme.jours_feries(dt_traitement) = 'true' and to_char(dt_traitement,'D') = 6 or to_char(dt_traitement,'D') = 7
            loop
                dt_traitement := dt_traitement + 1 ;
            end loop;

            utl_file.put_line(log_file,dbtab||'Prochain quantième: '|| dt_traitement);
            update quantieme q set q.quantieme = dt_traitement ;
            utl_file.put_line(log_file,ftab||to_char(sysdate,'yyyy/mm/dd-hh:mi:ss')||'--------------------------------');
        end p230_maj_quantieme ;

end charge_avis_pkg;