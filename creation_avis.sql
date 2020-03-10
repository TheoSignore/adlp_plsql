create or replace PACKAGE BODY         CREATION_AVIS AS

   procedure creer_les_avis is
   
        RetVal NUMBER;
        repert varchar2(100) :='/mnt/grfa/Systeme_outputdir/charge_avis/';
        seq int := 0;
        
        cursor crsr_param is
            select ec.type_offre as offre,
                ec.chrono_dest as chrono,
                pe.codsup as support
            from systeme.evenements_clients ec,
                systeme.produit_evenement pe
            where codestatut = 3
                and ec.code_produit = pe.code_produit
                and ec.num_ordre_type_evenement = pe.num_ordre_type_evenement
                and pe.codsup <> 'P'
            group by ec.type_offre, ec.chrono_dest, pe.codsup;  
                                    
        begin
            update systeme.evenements_clients set codestatut = 3 where codestatut = 1;
        
            select nvl(max(ID_PDF),0) into seq from pdf_fichier;
            
            for liste in crsr_param
            loop
            
                seq := seq + 1;
                
                insert into systeme.pdf_fichier(
                        ID_PDF,
                        CD_CHRONO,
                        CD_TYPEOFFR, 
                        DT_CREAPDF,
                        DT_ENVOPDF,
                        DT_ARRE,
                        DT_CREA, 
                        DT_MODI, 
                        CD_UTILCREA, 
                        CD_UTILMODI, 
                        LB_LIENFICH, 
                        CD_CODSUP
                    )
                    values(
                        seq,
                        liste.chrono,
                        liste.offre,
                        sysdate,
                        sysdate,
                        sysdate,
                        sysdate,
                        sysdate,
                        'THEO',
                        'THEO',
                        null,
                        liste.support
                    );
                    
                RetVal := SYSTEME.CREATIONFICHIER.EXTRAIREDONNEES ( liste.offre, liste.chrono, liste.support, repert );
                commit;
                
                repert := '/mnt/grfa/Systeme_outputdir/charge_avis/';
                
            end loop;
            
        end creer_les_avis ;
        
END CREATION_AVIS;