---- Script Generador de Saldos Iniciales (FINAL - SOPORTE AÑO Y CAMBIO DE AÑO) ----

-- Paso Previo: Asegurar que la tabla saldos_iniciales tenga la columna 'ano'
ALTER TABLE saldos_iniciales ADD COLUMN IF NOT EXISTS ano INT;

-- 1. Insertar saldos iniciales para semanas 2-5 del MISMO mes y MISMO año
INSERT INTO saldos_iniciales (saldoinicial, ano, mes, semana, banco, moneda_ref)
SELECT
    SUM(rs.saldofinal) AS saldoinicial,
    rs.ano,              -- El año se mantiene igual
    rs.mes,              -- El mes se mantiene igual
    rs.semana + 1 AS semana,
    rs.banco,
    rs.moneda_ref
FROM resumen_saldos rs
WHERE rs.semana BETWEEN 1 AND 3  -- Solo semanas 1-4 generan semanas 2-5
  AND NOT EXISTS (
    SELECT 1
    FROM saldos_iniciales si
    WHERE si.ano = rs.ano       -- Verificamos año
      AND si.mes = rs.mes
      AND si.semana = rs.semana + 1
      AND si.banco = rs.banco
      AND si.moneda_ref = rs.moneda_ref
  )
GROUP BY rs.ano, rs.mes, rs.semana, rs.banco, rs.moneda_ref;

-- 2. Insertar saldos iniciales para semana 1 del mes SIGUIENTE
-- (Maneja el cambio de mes normal Y el cambio de año: Dic -> Ene)
INSERT INTO saldos_iniciales (saldoinicial, ano, mes, semana, banco, moneda_ref)
SELECT
    SUM(rs.saldofinal) AS saldoinicial,
    
    -- Lógica de Año: Si es mes 12, sumamos 1 al año (2025->2026). Si no, queda igual.
    CASE 
        WHEN rs.mes = 12 THEN rs.ano + 1 
        ELSE rs.ano 
    END AS ano,

    -- Lógica de Mes: Si es mes 12, reseteamos a 1. Si no, sumamos 1.
    CASE 
        WHEN rs.mes = 12 THEN 1 
        ELSE rs.mes + 1 
    END AS mes,

    1 AS semana,          -- Siempre arranca en semana 1
    rs.banco,
    rs.moneda_ref

FROM resumen_saldos rs
WHERE rs.semana = 4  -- Solo la última semana del mes genera el nuevo mes
  AND NOT EXISTS (
    SELECT 1
    FROM saldos_iniciales si
    -- Verificamos contra los valores calculados (CASE)
    WHERE si.ano = (CASE WHEN rs.mes = 12 THEN rs.ano + 1 ELSE rs.ano END)
      AND si.mes = (CASE WHEN rs.mes = 12 THEN 1 ELSE rs.mes + 1 END)
      AND si.semana = 1
      AND si.banco = rs.banco
      AND si.moneda_ref = rs.moneda_ref
  )
GROUP BY rs.ano, rs.mes, rs.banco, rs.moneda_ref;