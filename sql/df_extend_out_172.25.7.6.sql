SELECT 
    call_date,
    phone_number_dialed,
    campaign_id,
    status,
    status_name,
    user,
    list_id,
    length_in_sec,
    lead_id,
    uniqueid,
    caller_code,
    IP_DESCARGA
FROM
    bbdd_groupcos_repositorio_planta_telefonica.`tb_marcaciones_vicidial_out_172.25.7.6`
WHERE
    campaign_id IN ('DELIMA' , 'DELBVEN',
        'DLRENGEN',
        'DELBANAC',
        'BANCA_IN',
        'BANCAOUT',
        'GMFENDOS',
        'GMFPLANC',
        'GMFDIREC',
        'DLRENAUT',
        'SERV_DEL')
        AND (call_date between CURRENT_TIMESTAMP() - INTERVAL 1 DAY and CURRENT_TIMESTAMP()) 