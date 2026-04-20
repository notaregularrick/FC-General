
--saldo incial de una semana en especifico, en este caso semana 2 del mes 3, se puede cambiar el mes y semana segun se necesite


INSERT INTO saldos_iniciales (
    saldoinicial,
    mes,
    semana,
    banco,
    moneda_ref,
    tasa_mes,
    fecha,
    saldo_usd,
    ano
)
SELECT
    rs.saldofinal AS saldoinicial,
    rs.mes,
    rs.semana + 1 AS semana,
    rs.banco,
    rs.moneda_ref,
    rs.tasa_mes,
    (rs.fecha + INTERVAL '7 days')::date AS fecha,
    rs.saldo_usd,
    rs.ano
FROM resumen_saldos rs
WHERE NOT EXISTS (
    SELECT 1
    FROM saldos_iniciales si
    WHERE si.ano        = rs.ano
      AND si.mes        = rs.mes
      AND si.semana     = rs.semana + 1
      AND si.banco      = rs.banco
      AND si.moneda_ref = rs.moneda_ref
)
and rs.mes=3 and rs.semana=1;