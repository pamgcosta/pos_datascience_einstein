INSERT INTO `aerobic-datum-330818.estudo_de_caso.dashboard`( 

WITH limp1 AS(
    SELECT SPLIT(string_field_0, ';')[OFFSET(0)] AS cnes
        , SPLIT(string_field_0, ';')[OFFSET(1)] AS uf
        , SPLIT(string_field_0, ';')[OFFSET(2)] AS nome
        , SPLIT(string_field_0, ';')[OFFSET(3)] AS logradouro
        , SPLIT(string_field_0, ';')[OFFSET(4)] AS bairro
        , SPLIT(string_field_0, ';')[OFFSET(5)] AS complemento
        , SPLIT(string_field_0, ';')[OFFSET(6)] AS latitude
        , SPLIT(string_field_0, ';')[OFFSET(7)] AS longitude
    FROM `aerobic-datum-330818.estudo_de_caso.cadastro_ubs`
)
, ajuste_cadastro_ubs as(
    SELECT uf 
        , COUNT(DISTINCT cnes) AS ubs_disp_uf
    FROM limp1
    WHERE CNES != 'CNES'
    GROUP BY uf
) 
, ajuste_sorologia as(
    SELECT sample
    , dt_collection
    , dt_birth  
    , age   
    , CASE city
        WHEN "Rio de Janeiro"
        THEN "33" -- Código RJ
        WHEN "Belo Horizonte"
        THEN "31" -- Código MG 
        ELSE "35" -- Código SP
    END AS uf 
    , city   
    , sex   
    , method1_cmia_screening    
    , method1_elisa_screening   
    , method2_immunoblot_confirmatory
    , method3_rtpcr_confirmatorY
FROM `aerobic-datum-330818.estudo_de_caso.sorologia`
)
SELECT CAST(s.sample AS INT64) AS sample
    , dt_collection
    , CAST(u.ubs_disp_uf AS INT64) AS ubs_disp_uf
    , s.dt_birth 
    , s.age 
    , s.city
    , s.uf    
    , s.sex   
    , s.method1_cmia_screening    
    , s.method1_elisa_screening  
    , s.method2_immunoblot_confirmatory
    , s.method3_rtpcr_confirmatorY
FROM ajuste_sorologia AS s
LEFT JOIN ajuste_cadastro_ubs AS u
    ON s.uf = u.uf
)