CREATE OR REPLACE PACKAGE FLUX_REL_NF_CANCELADAS_CPROC is

  -- Author        : Renato Senechal/Paulo Santos
  -- Created       : dd/mm/yyyy
  -- Purpose       : Relatorio de Notas Canceladas, Inutilizadas, Denegadas e Duplicadas

  FUNCTION Parametros RETURN VARCHAR2;
  FUNCTION Nome RETURN VARCHAR2;
  FUNCTION Tipo RETURN VARCHAR2;
  FUNCTION Versao RETURN VARCHAR2;
  FUNCTION Descricao RETURN VARCHAR2;
  FUNCTION Modulo RETURN VARCHAR2;
  FUNCTION Classificacao RETURN VARCHAR2;


  FUNCTION EXECUTAR (CANC_DUPLI_W  VARCHAR2,
                     COD_EMPRESA_W VARCHAR2,
                     COD_ESTAB_W   VARCHAR2,
                     DATA_INI_W    DATE,
                     DATA_FIM_W    DATE)
                     RETURN INTEGER;

END FLUX_REL_NF_CANCELADAS_CPROC;

/


CREATE OR REPLACE PACKAGE BODY FLUX_REL_NF_CANCELADAS_CPROC is

  MCOD_EMPRESA EMPRESA.COD_EMPRESA%TYPE;
  MUSUARIO     USUARIO_ESTAB.COD_USUARIO%TYPE;
  MCOD_ESTAB   ESTABELECIMENTO.COD_ESTAB%TYPE;

  FUNCTION Parametros RETURN VARCHAR2 IS
    PSTR VARCHAR2(5000);

  BEGIN

    MCOD_EMPRESA := LIB_PARAMETROS.RECUPERAR('EMPRESA');
    MCOD_ESTAB   := NVL(LIB_PARAMETROS.RECUPERAR('ESTABELECIMENTO'), '');
    MUSUARIO     := LIB_PARAMETROS.Recuperar('USUARIO');


    lib_proc.add_param(pstr,
                       'CANCELADAS/DUPLICADAS', --05
                       'varchar2',
                       'RadioButton',
                       'S',
                       '1',
                       null,
                       '1=CANCELADA,2=DUPLICADA'); 

    LIB_PROC.ADD_PARAM(PSTR,
                       'Empresa ',
                       'Varchar2',
                       'Combobox',
                       'S',
                       NULL,
                       NULL,
                       'SELECT EMP.COD_EMPRESA, EMP.COD_EMPRESA ||'' - ''|| EMP.RAZAO_SOCIAL FROM EMPRESA EMP WHERE cod_empresa = ''' || MCOD_EMPRESA || ''' ORDER BY 1 DESC');

    LIB_PROC.ADD_PARAM(PSTR,
                       'Estabelecimento ',
                       'Varchar2',
                       'Combobox',
                       'S',
                       NULL,
                       NULL,
                       'select DISTINCT cod_estab, cod_estab|| '' - '' ||razao_social
                          FROM estabelecimento WHERE cod_empresa = ''' || MCOD_EMPRESA || ''' ORDER BY 1'
                       ,papresenta => 'S'
                       ,phabilita => ':1 = 2');

    LIB_PROC.ADD_PARAM(PSTR,
                       'Data Inicio ',
                       'Date',
                       'Textbox',
                       'S',
                       NULL,
                       'dd/mm/yyyy');

    LIB_PROC.ADD_PARAM(PSTR,
                       'Data Fim ',
                       'Date',
                       'Textbox',
                       'S',
                       NULL,
                       'dd/mm/yyyy');

    RETURN PSTR;
  END;

  FUNCTION Nome RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Relatorio NFs Canceladas/Duplicadas';
  END;

  FUNCTION Tipo RETURN VARCHAR2 IS
  BEGIN
    RETURN 'PIS/COFINS';
  END;

  FUNCTION Versao RETURN VARCHAR2 IS
  BEGIN
    RETURN '1.0';
  END;

  FUNCTION Descricao RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Relatorio de NFs Canceladas/Duplicadas';
  END;

  FUNCTION Modulo RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Processos Customizados';
  END;

  FUNCTION Classificacao RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Processos Customizados';
  END;


PROCEDURE GERA_NF_CANC (COD_EMPRESA_W VARCHAR2,
                           DATA_INI_W    DATE,
                           DATA_FIM_W    DATE,
                           MPROC_ID_W    NUMBER,
                           MY_DIR_W      VARCHAR2) IS


BEGIN

   DECLARE
   /*DECLARACAO DE VARIAVEIS*/
   v_sql_string VARCHAR2(32767);
   P_NOME_REL   VARCHAR2(20);

  BEGIN

  P_NOME_REL := 'REL_CANC_COMPL';

    /*Armazena a string com o select do relatório*/
  v_sql_string := 'select 07 "ORIGEM",
                   x07.cod_estab,
                   x07.data_fiscal,
                   decode (x07.ident_modelo,''69'',''55'',''72'',''65'',''70'',''57'') "MODELO",
                   x07.movto_e_s,
                   ''="''||x07.num_docfis||''"'' as num_docfis,
                   ''="''||x07.serie_docfis||''"'' as serie_docfis,
                   ''="''||x07.num_autentic_nfe||''"'' as num_autentic_nfe,
                   decode(x07.ind_nfe_deneg_inut, 1, ''1'', 2,''2'',null, '' '') "ind_nfe_deneg_inu"
             from x07_docto_fiscal x07
             where x07.cod_empresa =  '''||COD_EMPRESA_W||'''
               and x07.data_fiscal between '''||DATA_INI_W||''' and '''||DATA_FIM_W||'''
               and x07.situacao = ''S''
               and x07.cod_class_doc_fis <> ''2''

            union all

            select 130 "ORIGEM",
                   cod_estab,
                   data_ref,
                   decode (ident_modelo,''69'',''55'',''72'',''65'',''70'',''57'') "MODELO",
                   movto_e_s,
                   ''="''||num_docfis_ini||''"'' as num_docfis_ini,                   
                   ''="''||serie_docfis||''"'' as serie_docfis,
                   ''="''||num_autentic_nfe||''"'' as num_autentic,
                   decode(ind_situacao, 1, ''1'', ''2'') "ind_situacao"
              from x130_nfe_denegada_inutilizada
             where cod_empresa = '''||COD_EMPRESA_W||'''               
               and data_ref between '''||DATA_INI_W||''' and '''||DATA_FIM_W||'''
             order by num_docfis'
             ;

  /*Faz o select e joga na planilha, iniciando na linha 1*/
  FLUX_EXPORT_TO_CSV(P_NOME_REL||'_'||MPROC_ID_W||'_'||MUSUARIO||'.csv', v_sql_string, MY_DIR_W);

  -- Call the procedure
    begin
    prc_move_dir(p_directory_source => '/ngs/app/msdfp/oracle_app_dir/MSAFGEMT/',
                 p_directory_dest => '/ngs/app/msdfp/oracle_app_dir/MSAF_TMP/');
    end;

  EXCEPTION
    WHEN OTHERS THEN
    LIB_PROC.ADD_LOG('Erro ao salvar o relatório em: ' || MY_DIR_W || ' - ' || SQLERRM, 1);
    RETURN;
  END;

END GERA_NF_CANC;

PROCEDURE GERA_NF_DUPLI (COD_EMPRESA_W VARCHAR2,
                           COD_ESTAB_W   VARCHAR2,
                           DATA_INI_W    VARCHAR2,
                           DATA_FIM_W    VARCHAR2,
                           MPROC_ID_W    NUMBER,
                           MY_DIR_W      VARCHAR2) IS


BEGIN

   DECLARE
   /*DECLARACAO DE VARIAVEIS*/
   v_sql_string VARCHAR2(32767);
   P_NOME_REL   VARCHAR2(30);
   P_ANOMES     VARCHAR2(10);
   P_NUM        NUMBER:=0;



  cursor geralistanf is
  select /*+ parallel(t1) */
         t1.cod_empresa,
         t1.cod_estab,
         t1.movto_e_S,
         t1.num_docfis,
         t1.serie_docfis,
         T1.SITUACAO,
         t1.data_emissao,
         t1.data_fiscal,
         t1.cod_class_doc_fis,
         t1.num_autentic_nfe,
         x04.cod_fis_jur,
         x04.razao_social,
         to_char(t1.data_fiscal,'YYYYMM') ANOMES
    from x07_docto_fiscal t1, x04_pessoa_fis_jur x04
   where t1.data_fiscal  between to_date('01/01/2016', 'dd/mm/yyyy')  and DATA_FIM_W
     and (t1.cod_estab, t1.num_autentic_nfe) in
         (select x07.cod_estab, x07.num_autentic_nfe
            from x07_docto_fiscal x07
           where x07.cod_estab = COD_ESTAB_W
             and x07.cod_empresa = COD_EMPRESA_W
             and length(x07.num_autentic_nfe) = 44
             and x07.data_fiscal <= DATA_FIM_W
           group by cod_estab, num_autentic_nfe
           having count(*) > 1)
     and t1.ident_fis_jur = x04.ident_fis_jur
     and (t1.num_autentic_nfe like '__23%' or t1.num_autentic_nfe like '__24%')
   order by num_docfis, data_fiscal;

c1 geralistanf%rowtype;

BEGIN
P_NOME_REL := 'Relatorio_NF_Duplicadas';
P_ANOMES   := '';

delete from DPL_TESTE;   
commit; 

  for c1 in geralistanf LOOP

      P_NUM := geralistanf%ROWCOUNT;

     IF last_day(c1.data_fiscal) = DATA_FIM_W THEN

         INSERT INTO DPL_TESTE r(r.num_docfis) VALUES(c1.num_docfis);


     END IF;   
     commit;
     P_ANOMES := c1.ANOMES;
  end loop; 


   if P_NUM > 0 THEN
    /*Armazena a string com o select do relatório*/
   v_sql_string := 'select 
                          ''="''||t1.cod_estab||''"'' as cod_estab,
                          t1.movto_e_s,
                          ''="''||t1.num_docfis||''"'' as num_docfis,
                          ''="''||t1.serie_docfis||''"'' as serie_docfis,
                          T1.SITUACAO,
                          TO_CHAR(t1.data_emissao,''DD/MM/YYYY'') "DATA_EMISSAO",
                          TO_CHAR(t1.data_fiscal,''DD/MM/YYYY'') "DATA_FISCAL",
                          t1.cod_class_doc_fis,
                          '' "''  ||t1.num_autentic_nfe||''" '' as num_autentic,
                          '' "''  ||x04.cod_fis_jur||''" '' as cod_fis_jur,
                          '' "''  ||x04.razao_social||''" '' as razao_social
                     from x07_docto_fiscal t1, x04_pessoa_fis_jur x04
                    where t1.data_fiscal  between to_date(''01/01/2016'', ''dd/mm/yyyy'') and  '''||DATA_FIM_W||'''
                      and t1.num_docfis in (select num_docfis from DPL_TESTE)
                      and (t1.cod_estab, t1.num_autentic_nfe) in
                           (select x07.cod_estab, x07.num_autentic_nfe
                              from x07_docto_fiscal x07
                             where x07.cod_estab  = '''||COD_ESTAB_W||'''
                               and x07.cod_empresa = '''||COD_EMPRESA_W||'''
                               and length(x07.num_autentic_nfe) = 44
                               and x07.data_fiscal <= '''||DATA_FIM_W||'''
                             group by cod_estab, num_autentic_nfe
                            having count(*) > 1)
                      and t1.ident_fis_jur = x04.ident_fis_jur
                      and (t1.num_autentic_nfe like ''__23%'' or t1.num_autentic_nfe like ''__24%'')
                      order by num_autentic_nfe' ; 


  /*Faz o select e joga na planilha, iniciando na linha 1*/
  FLUX_EXPORT_TO_CSV(P_NOME_REL||'_'||P_ANOMES||'_'||COD_ESTAB_W||'_'||MPROC_ID_W||'.csv', v_sql_string, MY_DIR_W);

   -- Call the procedure
    begin
    prc_move_dir(p_directory_source => '/ngs/app/msdfp/oracle_app_dir/MSAFGEMT/',
                 p_directory_dest => '/ngs/app/msdfp/oracle_app_dir/MSAF_TMP/');
    end;
    LIB_PROC.ADD_LOG('Total registros: '||P_NUM, 1);
   end if;

  EXCEPTION
    WHEN OTHERS THEN
    LIB_PROC.ADD_LOG('Erro ao salvar o relatório em: ' || MY_DIR_W || ' - ' || SQLERRM, 1);
    LIB_PROC.ADD_LOG('Total registros: '||P_NUM, 1);
    RETURN;
  END;

END GERA_NF_DUPLI;

FUNCTION EXECUTAR (CANC_DUPLI_W  VARCHAR2,
                   COD_EMPRESA_W VARCHAR2,
                   COD_ESTAB_W   VARCHAR2,
                   DATA_INI_W    DATE,
                   DATA_FIM_W    DATE) RETURN INTEGER IS


  MPROC_ID     INTEGER;


  V_EMPRESA    empresa.razao_social%type;
  V_RAZAO      estabelecimento.razao_social%type;
  diretorio    VARCHAR2(250);
  my_dir       varchar2(100);

  V_CNPJ    VARCHAR2(20);

  BEGIN

    my_dir := 'MSAFGEMT';
    --my_dir := 'MSAF_TMP';
    --my_dir := 'MSAF_GERAL';
        -- Cria Processo
    IF COD_EMPRESA_W IS NULL THEN
    MCOD_EMPRESA := LIB_PARAMETROS.RECUPERAR('EMPRESA');
    ELSE
    MCOD_EMPRESA := COD_EMPRESA_W;
    END IF;

    MPROC_ID := LIB_PROC.new('FLUX_REL_CANC_CPROC', 48, 150);
    LIB_PROC.ADD_LOG('Processo '||MPROC_ID,1);

  IF CANC_DUPLI_W = '1' THEN
     GERA_NF_CANC(MCOD_EMPRESA, DATA_INI_W, DATA_FIM_W, MPROC_ID, my_dir);
  ELSE
     GERA_NF_DUPLI(MCOD_EMPRESA, COD_ESTAB_W, DATA_INI_W, DATA_FIM_W, MPROC_ID, my_dir);
  END IF;



   LIB_PROC.ADD_LOG('Finalizado com sucesso - Relatório salvo em: '||my_dir,1);
   LIB_PROC.CLOSE();
   RETURN MPROC_ID;

END;

END FLUX_REL_NF_CANCELADAS_CPROC;


/
