BEGIN
   DECLARE
      v_hc_status_flag   NUMBER DEFAULT 0;
      v_farm_count   NUMBER ;
      v_triton_count   NUMBER ;
      --v_hc_report clob;
      --v_hc_status varchar2(50);
      CURSOR c_SRC_LKP_count
      IS
   select  rownum
       ,lkp.Source_System_Total
       ,lkp.entity_desc
       ,lkp.Data_Presence_farm
       ,lkp.Data_Presence_Triton
       ,lkp.comments
from
(WITH
w0 as
(select v_cycle_date_num as cycle_dt_num 
from dual
)
,wa as
(select x.* 
from FARM.FACT_BHC_INSTR_LOANS_CF x
      ,w0 y 
where x.cycle_dt_num = y.cycle_dt_num
and x.entity_desc='BOW'
)
,wa1 as
(select x.* 
from farm.fact_triton_loans_detail_feed x
    ,w0 y 
where x.cycle_dt_num = y.cycle_dt_num
and x.entity_desc='BOW'
)
,wb as
(
select distinct x.src_sys_cd 
                ,x.entity_desc 
from wrk_farm.farm_source_system_lkup x 
where x.current_flg ='Y' 
and x.product='LOANS'
and x.entity_desc='BOW' 
order by x.entity_desc
)
,wb1 as
(
select distinct x.src_sys_cd,entity_desc
from wb x
minus
select distinct y.src_sys_cd,entity_desc
from wa y
)
,wc as
(
select distinct x.src_sys_cd,entity_desc
from wb x
minus
select distinct y.src_sys_cd,entity_desc
from wa1 y
 )
,wd as
(select  a.src_sys_cd  as Source_System_Total
        ,a.entity_desc as Entity_desc
        ,b.src_sys_cd  as Data_Presence_farm
        ,c.src_sys_cd  as Data_Presence_Triton
from wb a
left outer join 
wb1 b on a.src_sys_cd = b.src_sys_cd
left outer join 
wc c on a.src_sys_cd = c.src_sys_cd
)
select d.Source_System_Total
       ,d.entity_desc
      ,case when d.Data_Presence_farm is not null then 'N' ELSE 'Y' END AS Data_Presence_Farm
      ,case when d.Data_Presence_Triton is not null then 'N' ELSE 'Y' END AS Data_Presence_Triton
      ,case when d.Data_Presence_farm is  null and d.Data_Presence_Triton is null then 'PASS' 
	   when d.Data_Presence_Triton is not null and d.Source_System_Total = 'MS' and <$$LAST_BUSINESS_DT_OF_MONTH> != v_cycle_date_num then 'NA'
	   else 'FAIL' end AS COMMENTS
from wd d order by 2) lkp;

   v_SRC_LKP_count    c_SRC_LKP_count%ROWTYPE;
   BEGIN
      v_hc_report := v_hc_report || CHR (10) || '  ';
      v_hc_report :=
            v_hc_report
         || CHR (10)
         || '           ************************************************************************************************************';
      v_hc_report :=
            v_hc_report
         || CHR (10)
         || '                 TRITON BOW Loans - Source Systems Presence Health Check Report for '
         || TO_CHAR (TO_DATE (v_cycle_date_num, 'yyyymmdd'), 'dd-MON-yyyy');
      v_hc_report :=
            v_hc_report
         || CHR (10)
         || '           ************************************************************************************************************';
      v_hc_report := v_hc_report || CHR (10) || '  ';
          
     v_hc_report :=
            v_hc_report
         || CHR (10)
         || '            Health Check for Source Systems Presence between Farm tables and Triton Tables';
      v_hc_report :=
            v_hc_report
         || CHR (10)
         || '           ----------------------------------------------------------------------------------------------------';
         
            
      v_hc_report :=
            v_hc_report
         || CHR (13)
         || '    No. Source Systems Name    Entity Desc    Data Presence in FARM(Y/N)    Data Presence in TRITON(Y/N)          Comments(Pass/Fail)';

      OPEN c_SRC_LKP_count;

      LOOP
         FETCH c_SRC_LKP_count INTO v_SRC_LKP_count;
         EXIT WHEN c_SRC_LKP_count%NOTFOUND;
         v_hc_report :=
               v_hc_report
            || CHR (13)
            || '    '
            || RPAD (LOWER (v_SRC_LKP_count.rownum), 6, ' ')
            || RPAD (v_SRC_LKP_count.Source_System_Total, 24, ' ')
            || RPAD (v_SRC_LKP_count.entity_desc, 27, ' ')
            || RPAD (v_SRC_LKP_count.Data_Presence_farm, 30, ' ')
            || RPAD (TO_CHAR (v_SRC_LKP_count.Data_Presence_Triton), 15)
            || LPAD (TO_CHAR (v_SRC_LKP_count.COMMENTS), 15);
            IF (v_SRC_LKP_count.Data_Presence_farm ='Y' and v_SRC_LKP_count.Data_Presence_Triton ='Y' ) or (v_SRC_LKP_count.COMMENTS = 'NA' and v_SRC_LKP_count.Source_System_Total = 'MS')
         THEN
            v_hc_status_flag := v_hc_status_flag + 0;
         ELSE
            v_hc_status_flag := v_hc_status_flag + 100;
         END IF;
            
      END LOOP;
      CLOSE c_SRC_LKP_count;

      IF     v_hc_status_flag = 0       
      THEN 
            v_hc_status := 'SUCCESSFUL';
          ELSE          
            v_hc_status := 'ABORT';
      END IF;
	  --DBMS_OUTPUT.PUT_LINE(v_hc_report);
      --DBMS_OUTPUT.PUT_LINE(v_hc_status);
   END;
EXCEPTION
   WHEN NO_DATA_FOUND
   THEN
      NULL;
   WHEN OTHERS
   THEN
      RAISE;
END;
-------------------
    
   file2
   -------------------
   CREATE OR REPLACE PROCEDURE BULK_UPDATE (TCPA_PRTY_INFO       CLOB,
                                         TCPA_ACCT_INFO       CLOB,
                                         STATUS           OUT VARCHAR2,
                                         STATUSMESSAGE    OUT VARCHAR2)
IS
   /*=====================================================================
   * Constants declaration begins
   *=====================================================================*/
   V_TCPA_PRTY_INFO   XMLTYPE;
   V_TCPA_ACCT_INFO   XMLTYPE;
   V_PARTYID          VARCHAR2 (100);
   V_UNAME            VARCHAR2 (100);
   V_PHSTATUS         VARCHAR2 (100);
   V_PHID             VARCHAR2 (100);
   V_PHSTATUS         VARCHAR2 (100);
   V_ACCTID           VARCHAR2 (100);
   V_A_PHID           VARCHAR2 (100);
   V_A_CDFLAG         VARCHAR2 (100);
   V_A_CDEDATE        VARCHAR2 (100);
   V_A_CDBID          VARCHAR2 (100);
   V_A_PHSTATUS       VARCHAR2 (100);
   V_A_SCFLAG         VARCHAR2 (100);
   V_A_TCFLAG         VARCHAR2 (100);
   V_A_CEDATE         VARCHAR2 (100);
   V_A_CCHANNEL       VARCHAR2 (100);
   V_A_TBID           VARCHAR2 (100);
   V_A_CSID           VARCHAR2 (100);
   V_COUNT            VARCHAR2 (100);
   i                  BINARY_INTEGER := 1;
   j                  BINARY_INTEGER := 1;
   acct               BINARY_INTEGER := 1;
/*=====================================================================
* Constants declaration ends
*=====================================================================*/
BEGIN
   IF TCPA_PRTY_INFO <> EMPTY_CLOB () AND TCPA_ACCT_INFO <> EMPTY_CLOB ()
   THEN
      V_TCPA_PRTY_INFO := XMLType (TCPA_PRTY_INFO);
      V_TCPA_ACCT_INFO := XMLType (TCPA_ACCT_INFO);

      /*TCPA PARTY Info*/
      IF V_TCPA_PRTY_INFO.EXISTSNODE ('/party/id[' || TO_CHAR (i) || ']') = 1
      THEN
         SELECT EXTRACTVALUE (VALUE (xml_list), '//id/text()')
           INTO V_PARTYID
           FROM TABLE (XMLSEQUENCE (EXTRACT (V_TCPA_PRTY_INFO, '/party/id')))
                xml_list;

         SELECT EXTRACTVALUE (VALUE (xml_list), '//userName/text()')
           INTO V_UNAME
           FROM TABLE (XMLSEQUENCE (EXTRACT (V_TCPA_PRTY_INFO, '/party')))
                xml_list;

         FOR C
            IN (SELECT EXTRACTVALUE (VALUE (xml_list), '//phid/text()')
                          AS V_PHID,
                       EXTRACTVALUE (VALUE (xml_list),
                                     '//phoneStatus/text()')
                          AS V_PHSTATUS
                  FROM TABLE (
                          XMLSEQUENCE (
                             EXTRACT (
                                V_TCPA_PRTY_INFO,
                                '/party/electronicAddresses/phoneNumber')))
                       xml_list)
         LOOP
            MERGE INTO ESB_PRTY P
                 USING (SELECT V_PARTYID    AS PARTYID,
                               V_UNAME      AS UNAME,
                               C.V_PHID     AS PHID,
                               C.V_PHSTATUS AS PHSTATUS
                          FROM DUAL) SP
                    ON (    TRIM (P.PARTYID) = TRIM (SP.PARTYID)
                        AND TRIM (P.ELECTRONICADDRESSKEYID) = TRIM (PHID)
                        AND TRIM (P.MSG_STAT_CD) = 'NEW')
            WHEN MATCHED
            THEN
               UPDATE SET
                  P.ELECTRONICADDRESSPHONESTATUS =
                     NVL (SP.PHSTATUS, P.ELECTRONICADDRESSPHONESTATUS),
                  UPDATE_DTTM = SYSDATE,
                  UPDATED_BY = SP.UNAME
            WHEN NOT MATCHED
            THEN
               INSERT     (PARTYID,
                           ELECTRONICADDRESSKEYID,
                           ELECTRONICADDRESSPHONESTATUS,
                           MSG_STAT_CD,
                           INSERT_DTTM,
                           CREATED_BY)
                   VALUES (SP.PARTYID,
                           SP.PHID,
                           SP.PHSTATUS,
                           'NEW',
                           SYSDATE,
                           SP.UNAME);

            --COMMIT;

            --            DBMS_OUTPUT.PUT_line (V_PARTYID);
            --            DBMS_OUTPUT.PUT_line (C.V_PHID);
            --            DBMS_OUTPUT.PUT_line (C.V_PHSTATUS);
            /*TCPA ACCT Info*/
            IF     V_TCPA_PRTY_INFO.EXISTSNODE (
                      '/party/id[' || TO_CHAR (j) || ']') = 1
               AND V_TCPA_ACCT_INFO.EXISTSNODE (
                      '/accounts/account/id[' || TO_CHAR (j) || ']') = 1
            THEN
               FOR C
                  IN (SELECT EXTRACTVALUE (VALUE (xml_list), '//id/text()')
                                AS V_ACCTID,
                             EXTRACTVALUE (VALUE (xml_list),
                                           '//ceaseDesistFlag/text()')
                                AS V_A_CDFLAG,
                             EXTRACTVALUE (VALUE (xml_list),
                                           '//ceaseDesistEffDt/text()')
                                AS V_A_CDEDATE,
                             EXTRACTVALUE (VALUE (xml_list),
                                           '//ceaseDesistBankerLanId/text()')
                                AS V_A_CDBID
                        FROM TABLE (
                                XMLSEQUENCE (
                                   EXTRACT (V_TCPA_ACCT_INFO,
                                            '/accounts/account'))) xml_list)
               LOOP
                  MERGE INTO ESB_ACCT P
                       USING (SELECT V_PARTYID    AS PARTYID,
                                     V_UNAME      AS UNAME,
                                     C.V_ACCTID   AS ACCTID,
                                     C.V_A_CDFLAG AS CDFLAG,
                                     TO_DATE (C.V_A_CDEDATE, 'YYYY-MM-DD')
                                        AS CDEDATE,
                                     C.V_A_CDBID  AS CDBID
                                FROM DUAL) SP
                          ON (    TRIM (P.PARTYID) = TRIM (SP.PARTYID)
                              AND TRIM (P.ACCOUNTID) = TRIM (SP.ACCTID)
                              AND TRIM (P.MSG_STAT_CD) = 'NEW')
                  WHEN MATCHED
                  THEN
                     UPDATE SET P.CEASEDESISTFLAG = SP.CDFLAG,
                                P.CEASEDESISTEFFDT = SP.CDEDATE,
                                P.BANKERLANID = SP.CDBID,
                                UPDATE_DTTM = SYSDATE,
                                UPDATED_BY = SP.UNAME
                  WHEN NOT MATCHED
                  THEN
                     INSERT     (PARTYID,
                                 ACCOUNTID,
                                 CEASEDESISTFLAG,
                                 CEASEDESISTEFFDT,
                                 BANKERLANID,
                                 MSG_STAT_CD,
                                 INSERT_DTTM,
                                 CREATED_BY)
                         VALUES (SP.PARTYID,
                                 SP.ACCTID,
                                 SP.CDFLAG,
                                 SP.CDEDATE,
                                 SP.CDBID,
                                 'NEW',
                                 SYSDATE,
                                 SP.UNAME);

                  --COMMIT;

                  /*TCPA ACCT ELECTRONICADDRESS Info*/

                  FOR S
                     IN (SELECT EXTRACTVALUE (VALUE (xml_list),
                                              '//phid/text()')
                                   AS V_A_PHID,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//phoneStatus/text()')
                                   AS V_A_PHSTATUS,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//smsConsentFlag/text()')
                                   AS V_A_SCFLAG,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//tcpaConsentFlag/text()')
                                   AS V_A_TCFLAG,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//consentEffDt/text()')
                                   AS V_A_CEDATE,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//consentChannel/text()')
                                   AS V_A_CCHANNEL,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//tcpaBankerLanId/text()')
                                   AS V_A_TBID,
                                EXTRACTVALUE (VALUE (xml_list),
                                              '//consentScriptId/text()')
                                   AS V_A_CSID
                           FROM TABLE (
                                   XMLSEQUENCE (
                                      EXTRACT (
                                         V_TCPA_ACCT_INFO,
                                            '/accounts/account['
                                         || TO_CHAR (acct)
                                         || ']/electronicAddresses/phoneNumber')))
                                xml_list)
                  LOOP
                     MERGE INTO ESB_ACCT_ELECADDR_CONSENTINFO P
                          USING (SELECT C.V_ACCTID     AS ACCTID,
                                        V_UNAME        AS UNAME,
                                        S.V_A_PHSTATUS AS PHSTATUS,
                                        S.V_A_PHID     AS PHID,
                                        S.V_A_SCFLAG   AS SCFLAG,
                                        S.V_A_TCFLAG   AS TCFLAG,
                                        TO_DATE (S.V_A_CEDATE, 'YYYY-MM-DD')
                                           AS CEDATE,
                                        S.V_A_CCHANNEL AS CCHANNEL,
                                        S.V_A_TBID     AS TBID,
                                        S.V_A_CSID     AS CSID
                                   FROM DUAL) SP
                             ON (    TRIM (P.ACCOUNTID) = TRIM (SP.ACCTID)
                                 AND TRIM (P.ELECTRONICADDRESSKEYID) =
                                        TRIM (PHID)
                                 AND TRIM (P.MSG_STAT_CD) = 'NEW')
                     WHEN MATCHED
                     THEN
                        UPDATE SET
                           P.ELECTRONICADDRESSPHONESTATUS =
                              NVL (SP.PHSTATUS,
                                   P.ELECTRONICADDRESSPHONESTATUS),
                           P.ELECTRONICADDRESSTCPACONSENFLG =
                              NVL (SP.TCFLAG,
                                   P.ELECTRONICADDRESSTCPACONSENFLG),
                           P.ELECTRONICADDRESSSMSCONSENFLAG =
                              NVL (SP.SCFLAG,
                                   P.ELECTRONICADDRESSSMSCONSENFLAG),
                           P.ELECTRONICADDRESSCONSENTCHANEL =
                              NVL (SP.CCHANNEL,
                                   P.ELECTRONICADDRESSCONSENTCHANEL),
                           P.ELECTRONICADDRESSCONSENTEFFDT =
                              NVL (SP.CEDATE,
                                   P.ELECTRONICADDRESSCONSENTEFFDT),
                           P.ELECTRONICADDRESSCONSENTSCTID =
                              NVL (SP.CSID, P.ELECTRONICADDRESSCONSENTSCTID),
                           P.ELECTRONICADDRESSTCPABANKENID =
                              NVL (SP.TBID, ELECTRONICADDRESSTCPABANKENID),
                           UPDATE_DTTM = SYSDATE,
                           UPDATED_BY = SP.UNAME
                     WHEN NOT MATCHED
                     THEN
                        INSERT     (ACCOUNTID,
                                    ELECTRONICADDRESSKEYID,
                                    ELECTRONICADDRESSPHONESTATUS,
                                    ELECTRONICADDRESSTCPACONSENFLG,
                                    ELECTRONICADDRESSSMSCONSENFLAG,
                                    ELECTRONICADDRESSCONSENTCHANEL,
                                    ELECTRONICADDRESSCONSENTEFFDT,
                                    ELECTRONICADDRESSCONSENTSCTID,
                                    ELECTRONICADDRESSTCPABANKENID,
                                    MSG_STAT_CD,
                                    INSERT_DTTM,
                                    CREATED_BY)
                            VALUES (SP.ACCTID,
                                    SP.PHID,
                                    SP.PHSTATUS,
                                    SP.TCFLAG,
                                    SP.SCFLAG,
                                    SP.CCHANNEL,
                                    SP.CEDATE,
                                    SP.CSID,
                                    SP.TBID,
                                    'NEW',
                                    SYSDATE,
                                    SP.UNAME);
                  END LOOP;

                  acct := acct + 1;
               END LOOP;

               j := j + 1;
            END IF;
         END LOOP;

         i := i + 1;
      END IF;

      COMMIT;

      STATUS := 'SUCCESS';
      STATUSMESSAGE := 'PROCEDURE COMPLETED SUCESSFULLY';
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      STATUS := 'FAILURE';
      STATUSMESSAGE := SUBSTR(SQLERRM, INSTR(SQLERRM, ':')+1);
END;


Sample Party XML:

<party>
<id>12345</id>
<electronicAddresses>
<!-- Repeated phoneNumbers>
<phoneNumber>
<id>p123</id>
<phoneStatus>ACTIVE</phoneStatus>
</phoneNumber>
</electronicAddresses>
</party>

Sample Account XML:

<accounts>
<!-- Repeated accounts>
<account>
<id>12345</id>
<ceaseDesistFlag></ceaseDesistFlag>
<ceaseDesistEffDt>2019-03-12</ceaseDesistEffDt>
<ceaseDesistBankerLanId>mannes01</ceaseDesistBankerLanId>

<electronicAddresses>
<!-- Repeated phoneNumbers>
<phoneNumber>
<id>p123</id>
<phoneStatus>ACTIVE</phoneStatus>
<tcpaConsentFlag>1<tcpaConsentFlag>
<smsConsentFlag>0</smsConsentFlag>
<consentChannel>CALL</consentChannel>
<consentEffDt>2019-03-12</consentEffDt>
<consentScriptId>1.0</consentScriptId>
<tcpaBankerLanId>mannes01</tcpaBankerLanId>
</phoneNumber>
</electronicAddresses>

</account>
</accounts>

----------------

file 3
   -----------------
    
    with 
w0 as 
(select  $$cycle_date as cycle_dt_num
,to_date($$cycle_date,'yyyymmdd') as cycle_dt
from dual)

,wss as(select w.wss_gdp_time,wss_ccy_code,wss_ccy_spotrate from $$schema_wss.wss_currencies w,w0 z
where w.cycle_dt_num = z.cycle_dt_num and w.wss_ccy_date = z.cycle_dt 
) 

/* For Swaps with at least one unsettled/active leg, previously settled leg need to be loaded on any given day */
/* Swaps - which has 2 (legs or records) namely SPOT and FORWARD */
,wa as
(select distinct c.record_no,to_number(d.swap_id) swap_id,c.multiply_divide,c.fx_rate,c.trade_date,c.fx_currency,c.foreign_amount,c.counterparty,c.cycle_dt_num,c.settle_date,c.fx_type,c.purchase_or_sale from $$schema_fhb_misc.tbl_fx_main_ihc c,
(select b.rec_min_no,b.rec_max_no,b.swap_id,a.* from  $$schema_fhb_misc.tbl_fx_main_ihc a,w0 e,
(select max(record_no) as rec_max_no,min(record_no) as rec_min_no,listagg(record_no) within group(order by fx_type desc) as swap_id,trade_date, fx_currency, foreign_amount,counterparty 
from  $$schema_fhb_misc.tbl_fx_main_ihc group by trade_date, fx_currency, foreign_amount,counterparty having count(1) > 1) b
where a.trade_date= b.trade_date and a.fx_currency= b.fx_currency and  a.foreign_amount= b.foreign_amount and a.counterparty = b.counterparty and a.settle_date > e.cycle_dt and a.trade_date < = e.cycle_dt) d
where c.trade_date= d.trade_date and c.fx_currency= d.fx_currency and  c.foreign_amount= d.foreign_amount and c.counterparty = d.counterparty and c.record_no = d.rec_max_no or c.record_no = d.rec_min_no order by record_no)

/* Non-Swaps - which has single (leg or record) */
,wb as
(select record_no,to_number(b.swap_id) swap_id,a.multiply_divide,a.fx_rate,a.trade_date,a.fx_currency,a.foreign_amount,a.counterparty,a.cycle_dt_num,a.settle_date,a.fx_type,a.purchase_or_sale from  $$schema_fhb_misc.tbl_fx_main_ihc a,w0 c,
(select 0 as swap_id,trade_date, fx_currency, foreign_amount,counterparty from  $$schema_fhb_misc.tbl_fx_main_ihc group by trade_date, fx_currency, foreign_amount,counterparty having count(1) = 1) b
where a.trade_date= b.trade_date and a.fx_currency= b.fx_currency and  a.foreign_amount= b.foreign_amount and a.counterparty = b.counterparty and a.settle_date > c.cycle_dt and a.trade_date < = c.cycle_dt
 order by a.record_no)

,wc as
(
select * from wa
union all
select * from wb)

,wz as ( select c.* 
,case when swap_id <> 0 then 'FXS'
     when swap_id = 0 and fx_type = 'Spot' then 'FXC'
     when swap_id = 0 and fx_type = 'Forward' then 'FXF'
else fx_type end as product
,case when swap_id = 0 then 'NA'
      when swap_id <> 0 and fx_type = 'Spot' then 'Y'
      when swap_id <> 0 and fx_type = 'Forward' then 'N'
end as near_leg_flg
,case when swap_id = 0 then 'NA'
      when swap_id <> 0 and fx_type = 'Spot' then 'N'
      when swap_id <> 0 and fx_type = 'Forward' then 'Y'
end as far_leg_flg
,case when swap_id = 0 then 'N'
      when swap_id <> 0 then 'Y'
end as swap_flg
,case when swap_id = 0 then 'N'
      when swap_id <> 0 then 'Y'
else 'N' end as active_swap_flg
,case when purchase_or_sale  = 'Sale' then fx_currency 
    else 'USD' end as ccy_sold
,case when purchase_or_sale  = 'Purchase' then fx_currency 
    else 'USD' end as ccy_bought
,case when purchase_or_sale = 'Purchase' then foreign_amount
     else case when multiply_divide = '/' then foreign_amount/fx_rate
               when multiply_divide = '*' then foreign_amount*fx_rate
          else 0 end
end as bought_amt
,case when purchase_or_sale = 'Sale' then foreign_amount
     else case when multiply_divide = '/' then (foreign_amount/fx_rate)
               when multiply_divide = '*' then (foreign_amount*fx_rate)
          else 0 end
end as sold_amt
,case when fx_type <> 'Spot' then  
     ((case when multiply_divide = '/' then foreign_amount/wss_ccy_spotrate 
            when multiply_divide = '*' then foreign_amount*wss_ccy_spotrate end) -
      (case when multiply_divide = '/' then foreign_amount/fx_rate 
            when multiply_divide = '*' then foreign_amount*fx_rate end))
    else 0 end as disc_market_value
from wc c
left outer join wss w
on wss_gdp_time  in (select max(wss_gdp_time) from wss w where w.wss_ccy_code = c.fx_currency group by w.wss_ccy_code) 
and w.wss_ccy_code = c.fx_currency)

select 
d.cycle_dt_num
,'FHB_FX' as src_sys_cd
,1 as entity_id
,'FHB' as entity_desc
,z.product
,record_no as src_instr_id
,swap_id
,record_no as transaction_id
,case when settle_date > cycle_dt then 'UNS' 
      else 'SET' end as status_flg
,z.near_leg_flg
,z.far_leg_flg
,case when settle_date > cycle_dt then 'Y' 
      else 'N' end as active_flg
,z.swap_flg
,z.active_swap_flg
,case when purchase_or_sale = 'Purchase' then 'B'
      when purchase_or_sale = 'Sale' then 'S'
    else 'UNK' end as deal_cd
,'UNK' as deal_type
,counterparty as cust_short_name
,counterparty as cust_full_name
,'Financial Services' as customer_type
,0 as deal_nos
,case when swap_id <> 0 then 'FX SWAP TRANSACTION'
      when swap_id = 0 and fx_type = 'Spot' then 'SPOT FX PRODUCT'
      when swap_id = 0 and fx_type = 'Forward' then 'FX FORWARD TRANSACTION'
    else fx_type end as product_desc
,'UNK' as portfolio
,'UNK' as trader
,to_date('19000101','yyyymmdd') as coa_dt
,trade_date as entry_dt
,trade_date as trade_dt
,settle_date as value_dt
,to_date('19000101','yyyymmdd') as cancel_dt
,z.ccy_sold
,z.ccy_bought
,z.bought_amt
,z.sold_amt
,fx_rate as rate
,disc_market_value
,1 as cross_rate_reval
,case when purchase_or_sale = 'Purchase' then 'B'
      when purchase_or_sale = 'Sale' then 'S'
else 'UNK' end as direction
,case when fx_type = 'Forward' then 150965 else -1 end as gl_acct_num_purchase
,case when fx_type = 'Forward' then 150966 else -1 end as gl_acct_num_sale
,case when ccy_bought = 'USD' then bought_amt else sold_amt end as purchase_amt
,case when ccy_sold = 'USD' then sold_amt*(-1) else bought_amt*(-1) end as sale_amt
,'CEL100' as eu_cpty_class_cd
,'CUL305' as us_cpty_class_cd
,'CUG003' as five_g_cpty_class_cd
,-1 as cust_mstr_id
,'NA' as cust_rmpm_id
,150965 as gl_map_key
,1 as company_num
,case when fx_type = 'Forward' then 30301 else -1 end as cost_ctr_num
,case when fx_type = 'Spot' then -1
      when fx_type <> 'Spot' and disc_market_value >= 0 then 151010 
  else 260105 end as gl_acct_num_mtm
,case when fx_type = 'Spot' then -1 
   else 30320 end as cost_ctr_num_mtm
,'N' as csa_agreement_flg
,'N' as isda_netting_flg
from wz z,w0 d
----------------------------------

---STG---

CREATE TABLE STG_LN.LN_PRTY_MATCH
(
CYCLE_DT_NUM	NUMBER,
LEXID	VARCHAR2(15 CHAR)	NOT NULL,
PRTY_ID	VARCHAR2(15 CHAR)	NOT NULL,
LN_SEARCH	VARCHAR2(50 CHAR),
REJ_FLG	VARCHAR2(1 CHAR),
BUS_FLG	VARCHAR2(1 CHAR),
NAME_SPLIT	VARCHAR2(1 CHAR),
MINOR_FLG	VARCHAR2(1 CHAR),
FOREIGN_ADDR	VARCHAR2(1 CHAR),
INCOMPLETE_ADDR	VARCHAR2(1 CHAR),
LEXID_SCORE	VARCHAR2(5 CHAR),
HHID	VARCHAR2(15 CHAR),
BEST_SSN	VARCHAR2(10 CHAR),
SSN_SCORE 	VARCHAR2(5 CHAR),
DISTANCE_FORMULA	VARCHAR2(10 CHAR),
BEST_DOB	VARCHAR2(10 CHAR),
DOB_SCORE 	VARCHAR2(5 CHAR),
BEST_ADDR_MATCH_CD	VARCHAR2(5 CHAR),
BEST_ADDR_CONFIDENCE_CD	VARCHAR2(1 CHAR),
NAME_FLG	VARCHAR2(1 CHAR),
NAME_MATCH_INPUT_FLG	VARCHAR2(10 CHAR),
NAME_MATCH_FLG	VARCHAR2(10 CHAR),
BEST_FIRST 	VARCHAR2(50 CHAR),
BEST_MIDDLE	VARCHAR2(50 CHAR),
BEST_LAST	VARCHAR2(50 CHAR),
BEST_SUFFIX	VARCHAR2(5 CHAR),
BEST_STREETADDR	VARCHAR2(100 CHAR),
BEST_CITY	VARCHAR2(50 CHAR),
BEST_STATE	VARCHAR2(5 CHAR),
BEST_ZIP	VARCHAR2(10 CHAR),
DATE_LAST_SEEN	NUMBER(6,0),
DATE_FIRST_SEEN	NUMBER(6,0),
NEW_BEST_FRST_NM 	VARCHAR2(50 CHAR),
NEW_BEST_MDL_NM  	VARCHAR2(50 CHAR),
NEW_BEST_LAST_NM	VARCHAR2(50 CHAR),
NEW_BEST_SUFFIX	VARCHAR2(5 CHAR),
NEW_BEST_STREETADDR	VARCHAR2(100 CHAR),
NEW_BEST_CITY	VARCHAR2(50 CHAR),
NEW_BEST_STATE	VARCHAR2(5 CHAR),
NEW_BEST_ZIP	VARCHAR2(10 CHAR),
NEW_DATE_LAST_SEEN	NUMBER(6,0),
NEW_DATE_FIRST_SEEN	NUMBER(6,0),
NEW_BEST_ADDR_MATCH_CD	VARCHAR2(5 CHAR),
NEW_BEST_ADDR_CONFIDENCE_CD	VARCHAR2(1 CHAR),
DOD	DATE,
DOD_MATCHCODE	VARCHAR2(50 CHAR),
DECEASED_SOURCE	VARCHAR2(1 CHAR),
V_OR_P	VARCHAR2(1 CHAR),
ADDR_DESC_1	VARCHAR2(100 CHAR),
ADDR_DESC_2	VARCHAR2(100 CHAR),
ADDR_DESC_3	VARCHAR2(100 CHAR),
ADDR_DESC_4	VARCHAR2(100 CHAR),
ADDR_DESC_5	VARCHAR2(100 CHAR),
ADDR_DESC_6	VARCHAR2(100 CHAR),
ADDR_DESC_7	VARCHAR2(100 CHAR),
CMRA_INPUT_FLG	VARCHAR2(1 CHAR),
BUS_OR_RES_FLG	VARCHAR2(1 CHAR),
ADDR_TYPE_FLG	VARCHAR2(1 CHAR),
VACANCY_FLG	VARCHAR2(1 CHAR),
DND_FLG	VARCHAR2(1 CHAR),
POBOX_INPUT_FLG	VARCHAR2(1 CHAR),
SUBJ_FRST_NM_1	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_1	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_1	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_1	VARCHAR2(5 CHAR),
SUBJ_PHONE_1	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NM_1	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_1	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_1	DATE,
SUBJ_DATE_LAST_SEEN_1	DATE,
SUBJ_PHN_TYPE_1	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_1	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_1	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_1	VARCHAR2(10 CHAR),
SUBJ_FRST_NM_2	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_2	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_2	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_2	VARCHAR2(5 CHAR),
SUBJ_PHONE_2	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NAME_2	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_2	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_2	DATE,
SUBJ_DATE_LAST_SEEN_2	DATE,
SUBJ_PHN_TYPE_2	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_2	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_2	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_2	VARCHAR2(10 CHAR),
SUBJ_FRST_NM_3	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_3	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_3	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_3	VARCHAR2(5 CHAR),
SUBJ_PHONE_3	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NAME_3	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_3	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_3	DATE,
SUBJ_DATE_LAST_SEEN_3	DATE,
SUBJ_PHN_TYPE_3	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_3	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_3	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_3	VARCHAR2(10 CHAR),
INPUT_SWITCH_TYPE_1	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_2	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_3	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_4	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_5	VARCHAR2(1 CHAR),
ORIG_EMAIL_1	VARCHAR2(50 CHAR),
ORIG_EMAIL_2	VARCHAR2(50 CHAR),
ORIG_EMAIL_3	VARCHAR2(50 CHAR),
ORIG_EMAIL_4	VARCHAR2(50 CHAR),
ORIG_EMAIL_5	VARCHAR2(50 CHAR),
EMAIL_FLG	VARCHAR2(1 CHAR),
ALERT_SECURITY_FREEZE	VARCHAR2(50 CHAR),
ALERT_SECURITY_FRAUD	VARCHAR2(50 CHAR),
ALERT_IDENTITY_THREAT	VARCHAR2(50 CHAR),
ALERT_CONSUMER_STMT	VARCHAR2(50 CHAR),
INSERT_DTTM                   	TIMESTAMP(6),
INSERT_BATCH_RUN_ID	NUMBER	
)
PARTITION BY RANGE (CYCLE_DT_NUM)
(  
  PARTITION P_19010101 VALUES LESS THAN (19010101)
);

CREATE UNIQUE INDEX STG_LN.PK_LN_PRTY_MATCH ON STG_LN.LN_PRTY_MATCH
(CYCLE_DT_NUM,LEXID)
  LOCAL;

ALTER TABLE STG_LN.LN_PRTY_MATCH ADD (
  CONSTRAINT PK_LN_PRTY_MATCH
  PRIMARY KEY
  (CYCLE_DT_NUM,LEXID) 
  USING INDEX LOCAL
  ENABLE VALIDATE);

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO AUDITDATA;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO BIDMDA_RO;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO DATASTORE_LN;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO EDW_RO;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO EDW_VAL_ROLE;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO RBG_RO_O;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO RBG_RO_R;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO STG_LN_RO;

GRANT SELECT ON STG_LN.LN_PRTY_MATCH TO COMPLIANCE_RO_R;

GRANT STG_LN_RO TO SV_MDM;

---DATASTORE--

CREATE TABLE DATASTORE_LN.LN_PRTY_MATCH
(
CYCLE_DT_NUM	NUMBER,
LEXID	VARCHAR2(15 CHAR)	NOT NULL,
PRTY_ID	VARCHAR2(15 CHAR)	NOT NULL,
LN_SEARCH	VARCHAR2(50 CHAR),
REJ_FLG	VARCHAR2(1 CHAR),
BUS_FLG	VARCHAR2(1 CHAR),
NAME_SPLIT	VARCHAR2(1 CHAR),
MINOR_FLG	VARCHAR2(1 CHAR),
FOREIGN_ADDR	VARCHAR2(1 CHAR),
INCOMPLETE_ADDR	VARCHAR2(1 CHAR),
LEXID_SCORE	VARCHAR2(5 CHAR),
HHID	VARCHAR2(15 CHAR),
BEST_SSN	VARCHAR2(10 CHAR),
SSN_SCORE 	VARCHAR2(5 CHAR),
DISTANCE_FORMULA	VARCHAR2(10 CHAR),
BEST_DOB	VARCHAR2(10 CHAR),
DOB_SCORE 	VARCHAR2(5 CHAR),
BEST_ADDR_MATCH_CD	VARCHAR2(5 CHAR),
BEST_ADDR_CONFIDENCE_CD	VARCHAR2(1 CHAR),
NAME_FLG	VARCHAR2(1 CHAR),
NAME_MATCH_INPUT_FLG	VARCHAR2(10 CHAR),
NAME_MATCH_FLG	VARCHAR2(10 CHAR),
BEST_FIRST 	VARCHAR2(50 CHAR),
BEST_MIDDLE	VARCHAR2(50 CHAR),
BEST_LAST	VARCHAR2(50 CHAR),
BEST_SUFFIX	VARCHAR2(5 CHAR),
BEST_STREETADDR	VARCHAR2(100 CHAR),
BEST_CITY	VARCHAR2(50 CHAR),
BEST_STATE	VARCHAR2(5 CHAR),
BEST_ZIP	VARCHAR2(10 CHAR),
DATE_LAST_SEEN	NUMBER(6,0),
DATE_FIRST_SEEN	NUMBER(6,0),
NEW_BEST_FRST_NM 	VARCHAR2(50 CHAR),
NEW_BEST_MDL_NM  	VARCHAR2(50 CHAR),
NEW_BEST_LAST_NM	VARCHAR2(50 CHAR),
NEW_BEST_SUFFIX	VARCHAR2(5 CHAR),
NEW_BEST_STREETADDR	VARCHAR2(100 CHAR),
NEW_BEST_CITY	VARCHAR2(50 CHAR),
NEW_BEST_STATE	VARCHAR2(5 CHAR),
NEW_BEST_ZIP	VARCHAR2(10 CHAR),
NEW_DATE_LAST_SEEN	NUMBER(6,0),
NEW_DATE_FIRST_SEEN	NUMBER(6,0),
NEW_BEST_ADDR_MATCH_CD	VARCHAR2(5 CHAR),
NEW_BEST_ADDR_CONFIDENCE_CD	VARCHAR2(1 CHAR),
DOD	DATE,
DOD_MATCHCODE	VARCHAR2(50 CHAR),
DECEASED_SOURCE	VARCHAR2(1 CHAR),
V_OR_P	VARCHAR2(1 CHAR),
ADDR_DESC_1	VARCHAR2(100 CHAR),
ADDR_DESC_2	VARCHAR2(100 CHAR),
ADDR_DESC_3	VARCHAR2(100 CHAR),
ADDR_DESC_4	VARCHAR2(100 CHAR),
ADDR_DESC_5	VARCHAR2(100 CHAR),
ADDR_DESC_6	VARCHAR2(100 CHAR),
ADDR_DESC_7	VARCHAR2(100 CHAR),
CMRA_INPUT_FLG	VARCHAR2(1 CHAR),
BUS_OR_RES_FLG	VARCHAR2(1 CHAR),
ADDR_TYPE_FLG	VARCHAR2(1 CHAR),
VACANCY_FLG	VARCHAR2(1 CHAR),
DND_FLG	VARCHAR2(1 CHAR),
POBOX_INPUT_FLG	VARCHAR2(1 CHAR),
SUBJ_FRST_NM_1	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_1	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_1	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_1	VARCHAR2(5 CHAR),
SUBJ_PHONE_1	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NM_1	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_1	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_1	DATE,
SUBJ_DATE_LAST_SEEN_1	DATE,
SUBJ_PHN_TYPE_1	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_1	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_1	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_1	VARCHAR2(10 CHAR),
SUBJ_FRST_NM_2	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_2	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_2	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_2	VARCHAR2(5 CHAR),
SUBJ_PHONE_2	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NAME_2	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_2	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_2	DATE,
SUBJ_DATE_LAST_SEEN_2	DATE,
SUBJ_PHN_TYPE_2	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_2	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_2	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_2	VARCHAR2(10 CHAR),
SUBJ_FRST_NM_3	VARCHAR2(50 CHAR),
SUBJ_MDL_NM_3	VARCHAR2(50 CHAR),
SUBJ_LAST_NM_3	VARCHAR2(50 CHAR),
SUBJ_SUFFIX_3	VARCHAR2(5 CHAR),
SUBJ_PHONE_3	VARCHAR2(10 CHAR),
SUBJ_PHN_LISTING_NAME_3	VARCHAR2(120 CHAR),
SUBJ_PHN_POSSIBLE_RELTNSHIP_3	VARCHAR2(50 CHAR),
SUBJ_DATE_FIRST_SEEN_3	DATE,
SUBJ_DATE_LAST_SEEN_3	DATE,
SUBJ_PHN_TYPE_3	VARCHAR2(5 CHAR),
SUBJ_PHN_DUP_TO_INPUT_FLG_3	VARCHAR2(1 CHAR),
SUBJ_PHN_LINE_TYPE_3	VARCHAR2(1 CHAR),
SUBJ_PHN_PORTED_DATE_3	VARCHAR2(10 CHAR),
INPUT_SWITCH_TYPE_1	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_2	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_3	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_4	VARCHAR2(1 CHAR),
INPUT_SWITCH_TYPE_5	VARCHAR2(1 CHAR),
ORIG_EMAIL_1	VARCHAR2(50 CHAR),
ORIG_EMAIL_2	VARCHAR2(50 CHAR),
ORIG_EMAIL_3	VARCHAR2(50 CHAR),
ORIG_EMAIL_4	VARCHAR2(50 CHAR),
ORIG_EMAIL_5	VARCHAR2(50 CHAR),
EMAIL_FLG	VARCHAR2(1 CHAR),
ALERT_SECURITY_FREEZE	VARCHAR2(50 CHAR),
ALERT_SECURITY_FRAUD	VARCHAR2(50 CHAR),
ALERT_IDENTITY_THREAT	VARCHAR2(50 CHAR),
ALERT_CONSUMER_STMT	VARCHAR2(50 CHAR),
INSERT_DTTM                   	TIMESTAMP(6),
INSERT_BATCH_RUN_ID	NUMBER	
)
PARTITION BY RANGE (CYCLE_DT_NUM)
(  
  PARTITION P_19010101 VALUES LESS THAN (19010101)
);

CREATE UNIQUE INDEX DATASTORE_LN.PK_LN_PRTY_MATCH ON DATASTORE_LN.LN_PRTY_MATCH
(CYCLE_DT_NUM,LEXID)
  LOCAL;

ALTER TABLE DATASTORE_LN.LN_PRTY_MATCH ADD (
  CONSTRAINT PK_LN_PRTY_MATCH
  PRIMARY KEY
  (CYCLE_DT_NUM,LEXID)
  USING INDEX LOCAL
  ENABLE VALIDATE);

;
--------------------------------------
    
    WITH BASE
     AS (  SELECT C_PRTY_RLTD_ORG_XREF.ROWID_OBJECT,
                  C_PRTY_RLTD_ORG_XREF.ORG_UNIT_ID,
                  C_PRTY_RLTD_ORG.ORG_UNIT_ID AS ORG_UNIT_ID_SRC,
                  C_PRTY_RLTD_ORG.LAST_UPDATE_DATE,
                  C_PRTY_XREF.BUS_ACTV_TYP_CD,
                  CASE
                     WHEN TRIM (C_PRTY_RLTD_ORG_XREF.ROWID_SYSTEM) IN
                             ('CD', 'EQ', 'WA', 'CA', 'TP', 'DA')
                     THEN
                        'CD'
                     ELSE
                        TRIM (C_PRTY_RLTD_ORG_XREF.ROWID_SYSTEM)
                  END
                     AS ROWID_SYSTEM,
                  C_PRTY_XREF.CUST_LCYCL_STAT_CD,
                  C_PRTY_XREF.RLTD_PRTY_LCYCL_STAT_CD,
                  REGEXP_SUBSTR (C_PRTY_XREF.CUST_LCYCL_STAT_CD, '[^~]+$')
                     AS CUST_LCYCL_STATUS,
                  REGEXP_SUBSTR (C_PRTY_XREF.RLTD_PRTY_LCYCL_STAT_CD, '[^~]+$')
                     AS RLTD_LCYCL_STATUS,
                  C_PRTY_RLTD_ORG_XREF.PRTY_RLTD_ORG_END_DT
             FROM BOW_ORS.C_PRTY_RLTD_ORG_XREF,
                  BOW_ORS.C_PRTY_RLTD_ORG,
                  BOW_ORS.C_PRTY_XREF,
                  BOW_ORS.C_PRTY
            WHERE     C_PRTY_RLTD_ORG_XREF.ROWID_OBJECT =
                         C_PRTY_RLTD_ORG.ROWID_OBJECT
                  AND C_PRTY_XREF.ROWID_XREF = C_PRTY_RLTD_ORG_XREF.PRTY_ID
                  AND C_PRTY.ROWID_OBJECT = C_PRTY_RLTD_ORG.PRTY_ID
                  AND C_PRTY.CONSOLIDATION_IND IN (1, 2) /*AND C_PRTY_RLTD_ORG.PRTY_ID = '20000095'*/
--                  AND (   C_PRTY_RLTD_ORG.LAST_UPDATE_DATE >=
--                             TO_DATE ('07/24/2018 13:21:54',
--                                      'MM:DD:YYYY HH24:MI:SS')
--                       OR C_PRTY_RLTD_ORG_XREF.LAST_UPDATE_DATE >=
--                             TO_DATE ('07/24/2018 13:21:54',
--                                      'MM:DD:YYYY HH24:MI:SS')
--                       OR C_PRTY_XREF.LAST_UPDATE_DATE >=
--                             TO_DATE ('07/24/2018 13:21:54',
--                                      'MM:DD:YYYY HH24:MI:SS'))
                  AND C_PRTY_RLTD_ORG_XREF.ROWID_SYSTEM NOT IN ('PM', 'MD')
                  AND C_PRTY_RLTD_ORG_XREF.PRTY_RLTD_ORG_TYP_CD =
                         'PRTYORGTYP~LOB'
                  AND NOT (    TRIM (C_PRTY_RLTD_ORG_XREF.ROWID_SYSTEM) IN
                                  ('CD', 'EQ', 'WA', 'CA', 'TP')
                           AND C_PRTY_XREF.CUST_LCYCL_STAT_CD =
                                  'PTYLIFCYCSTAT~CLOSED')
                  AND C_PRTY_RLTD_ORG_XREF.HUB_STATE_IND = 1
                  AND C_PRTY_RLTD_ORG.HUB_STATE_IND = 1
                  AND C_PRTY_XREF.HUB_STATE_IND = 1
                  AND C_PRTY.HUB_STATE_IND = 1
         ORDER BY C_PRTY_RLTD_ORG_XREF.ROWID_OBJECT),
     FLG
     AS (SELECT /*+PARALLEL(5)*/ CDD_CNT,
                END_DT,
                CASE
                   WHEN    (    ROWID_SYSTEM != 'CD'
                            AND END_DT = 1
                            AND PRTY_RLTD_ORG_END_DT =
                                   TO_DATE (99991231, 'YYYYMMDD'))
                        OR (ROWID_SYSTEM != 'CD' AND END_DT = 0)
                        OR (ROWID_SYSTEM = 'CD')
                   THEN
                      1
                   ELSE
                      0
                END
                   AS PRIOR_FLG,
                B.*
           FROM BASE B,
                (  SELECT 
                         ROWID_OBJECT,
                          SUM (
                             CASE WHEN ROWID_SYSTEM LIKE 'CD' THEN 1 ELSE 0 END)
                             CDD_CNT,
                          COUNT (ROWID_SYSTEM) AS SYS_CNT,
                          SUM (
                             CASE WHEN ROWID_SYSTEM LIKE 'CM' THEN 1 ELSE 0 END)
                             CM_CNT,
                          MAX (
                             CASE
                                WHEN PRTY_RLTD_ORG_END_DT =
                                        TO_DATE (99991231, 'YYYYMMDD')
                                THEN
                                   1
                                ELSE
                                   0
                             END)
                             END_DT
                     FROM BASE
                 GROUP BY ROWID_OBJECT) F
          WHERE     B.ROWID_OBJECT = F.ROWID_OBJECT
                AND NOT (SYS_CNT = 1 AND CM_CNT = 1)),
     HIER
     AS (SELECT H.*,
                DENSE_RANK () OVER (PARTITION BY ROWID_OBJECT ORDER BY HIER)
                   AS HIER_RANK
           FROM (SELECT ROWID_OBJECT,
                        ORG_UNIT_ID,
                        ORG_UNIT_ID_SRC,
                        LAST_UPDATE_DATE,
                        BUS_ACTV_TYP_CD,
                        ROWID_SYSTEM,
                        CDD_CNT,
                        CUST_LCYCL_STATUS,
                        RLTD_LCYCL_STATUS,
                        (CASE
                            WHEN     CDD_CNT = 1
                                 AND NOT (   CUST_LCYCL_STATUS = 'NA'
                                          OR CUST_LCYCL_STATUS IS NULL)
                                 AND ROWID_SYSTEM = 'CD'
                            THEN
                               1
                            WHEN     CDD_CNT > 1
                                 AND NOT (   CUST_LCYCL_STATUS = 'NA'
                                          OR CUST_LCYCL_STATUS IS NULL)
                                 AND ROWID_SYSTEM = 'CD'
                            THEN
                               2
                            WHEN     ROWID_SYSTEM NOT IN
                                        ('CM', 'PM', 'MD', 'HV', 'UI')
                                 AND CUST_LCYCL_STATUS = 'OPEN'
                            THEN
                               3
                            WHEN     ROWID_SYSTEM NOT IN
                                        ('CM', 'PM', 'MD', 'HV', 'UI')
                                 AND CUST_LCYCL_STATUS = 'CLOSED'
                            THEN
                               4
                            WHEN     CDD_CNT = 1
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND ROWID_SYSTEM = 'CD'
                                 AND RLTD_LCYCL_STATUS = 'OPEN'
                            THEN
                               5
                            WHEN     CDD_CNT > 1
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND ROWID_SYSTEM = 'CD'
                                 AND RLTD_LCYCL_STATUS = 'OPEN'
                            THEN
                               6
                            WHEN     CDD_CNT > 1
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND ROWID_SYSTEM = 'CD'
                                 AND RLTD_LCYCL_STATUS = 'CLOSED'
                            THEN
                               7
                            WHEN     ROWID_SYSTEM NOT IN
                                        ('CM', 'PM', 'MD', 'HV', 'UI')
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND RLTD_LCYCL_STATUS = 'OPEN'
                            THEN
                               8
                            WHEN     ROWID_SYSTEM NOT IN
                                        ('CM', 'PM', 'MD', 'HV', 'UI')
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND RLTD_LCYCL_STATUS = 'CLOSED'
                            THEN
                               9
                            WHEN     ROWID_SYSTEM NOT IN
                                        ('CM', 'PM', 'MD', 'HV', 'UI')
                                 AND (   CUST_LCYCL_STATUS = 'NA'
                                      OR CUST_LCYCL_STATUS IS NULL)
                                 AND (   RLTD_LCYCL_STATUS = 'NA'
                                      OR RLTD_LCYCL_STATUS IS NULL)
                            THEN
                               10
                         END)
                           AS HIER
                   FROM FLG WHERE PRIOR_FLG = 1) H)
SELECT ROWID_OBJECT,
       CASE WHEN HIER IN (4, 7, 9) THEN ORG_UNIT_ID_SRC ELSE ORG_UNIT_ID END
          AS ORG_UNIT_ID,
       LAST_UPDATE_DATE,
       BUS_ACTV_TYP_CD,
       ROWID_SYSTEM,
       HIER
  FROM HIER
 WHERE HIER_RANK = 1 
 ORDER BY ROWID_OBJECT
 ------------------------------------
 
           
