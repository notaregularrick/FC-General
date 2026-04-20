--Movimientos faltantes en Banesco Panama y Banesco 99

--INSERT INTO banesco99_bs_norm values (2025-10-17,10,3,0,'saldo faltante',2089.38,12227930.41,'BS',203.742,10.255,null,null,'DEBITO','BANESCO99',false,12230019.79);

--INSERT into  banescopanama_us_norm values (126,2025-10-06,10,2,'Genérico','Faltante',-3.50,0.00,'US',1.0000,3.50,null,null,'NEUTRA','BANESCOPANAMA',default,false,154294.36);

--insert into public.banplus_bs_norm (id, fecha, ano, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado)
--values  (212, '2026-01-01', 2026, 1, 1, '08978676', 'BRECHA DICIEMBRE-ENERO', -41884.50, 137806.09, 'BS', 0.0000, 0.00, null, null, 'DEBITO', 'BANPLUS', '2026-02-04 16:44:22.596082', true, 137806.09);


-----Scripts de Banplus (CON AÑO 2025-2026) -----------------------------------------------

-- Paso 1: Crear columnas (sin cambios)
ALTER TABLE banplus_bs_norm ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;
ALTER TABLE banplus_bs_norm ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANPLUS'
      AND moneda_ref = 'BS'
    ORDER BY fecha
    LIMIT 1
),
SumasPorTiempo AS (
    -- CAMBIO 1: Agregamos 'ano' a la selección y al agrupamiento
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM banplus_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano, -- CAMBIO 2: Pasamos el dato del año
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- CAMBIO 3: El orden debe ser Año -> Mes -> Semana
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización
UPDATE banplus_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- Mantenemos la corrección de redondeo y tipo numeric que hicimos antes
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- CAMBIO 4: El match ahora requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

SELECT saldoinicial, fecha
FROM saldos_iniciales
WHERE banco = 'BANPLUS' AND moneda_ref = 'BS'
ORDER BY fecha ASC LIMIT 1;

----------Scripts de Mercantil en Bolivares (FINAL - AÑO, FECHA Y DECIMALES) ---------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE mercantil_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE mercantil_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para Mercantil BS
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'MERCANTIL'
      AND moneda_ref = 'BS'
    -- MEJORA: Aseguramos tomar el saldo más antiguo cronológicamente
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM mercantil_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Soporte 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada
UPDATE mercantil_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Redondeo y conversión a numeric para comparación exacta
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts de Mercantil Panama (FINAL - AÑO, FECHA Y DECIMALES) ----------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE mercantilpanama_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE mercantilpanama_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para Mercantil Panamá USD
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'MERCANTILPANAMA' -- OJO: Verifica si lleva espacio o no en tu tabla
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el saldo más antiguo automáticamente
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM mercantilpanama_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE mercantilpanama_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación robusta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;


---Scripts de Bancamiga BS (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columnas (sin cambios)
ALTER TABLE bancamiga_bs_norm ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;
ALTER TABLE bancamiga_bs_norm ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

WITH SaldoInicial AS (
    -- Tomamos el saldo inicial basado en la fecha más antigua
    SELECT saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANCAMIGA'
      AND moneda_ref = 'BS'
    -- MEJORA: Orden cronológico para garantizar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bancamiga_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano,
        s.mes,
        s.semana,

        -- Cálculo del acumulado continuo
        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
UPDATE bancamiga_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND para evitar errores flotantes
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de BNC (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bnc_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bnc_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BNC'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bnc_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular acumulado respetando el orden cronológico (Año -> Mes -> Semana)
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bnc_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de BNC (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bnc6550_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bnc6550_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BNC6550'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bnc6550_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular acumulado respetando el orden cronológico (Año -> Mes -> Semana)
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bnc6550_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de BNC us (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bnc_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bnc_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BNC'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bnc_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular acumulado respetando el orden cronológico (Año -> Mes -> Semana)
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bnc_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;


-----Scripts de Provincial Bs (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE provincial_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE provincial_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para Provincial BS
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'PROVINCIAL'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM provincial_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con doble llave
UPDATE provincial_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts del BDV (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bdv_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bdv_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para BDV
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BDV'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bdv_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bdv_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de Bancamiga US (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bancamiga_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bancamiga_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para BANCAMIGA US
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANCAMIGA'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bancamiga_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bancamiga_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts de Mercantil US (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE mercantil_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE mercantil_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para MERCANTIL US
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'MERCANTIL'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM mercantil_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE mercantil_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND para evitar errores flotantes
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts de Provincial US (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE provincial_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE provincial_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para Provincial US
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'PROVINCIAL'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM provincial_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE provincial_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts de Banesco Verde (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE banescoverde_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE banescoverde_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para BANESCOVERDE US
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANESCOVERDE'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM banescoverde_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE banescoverde_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

-----Scripts de Banesco Planta (FINAL - AÑO, FECHA Y DECIMALES) ------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE banescoplanta_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE banescoplanta_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para BANESCOPLANTA BS
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANESCOPLANTA'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM banescoplanta_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE banescoplanta_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;




-- Paso 0:
    --insert into banesco99_bs_norm VALUES ('2025-10-17',10,3,000000,'saldo faltante',-2089.38,12227930.41,'BS',203.742,10.2550,'','','DEBITO','BANESCO99','false',12227930.41);
-----Scripts de Banesco99 (FINAL - AÑO, FECHA Y DECIMALES) -------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE banesco99_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE banesco99_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BANESCO99'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM banesco99_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
UPDATE banesco99_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- Mantenemos tu corrección de numeric que ya estaba correcta
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

WITH SaldoInicial AS (
    SELECT saldoinicial FROM saldos_iniciales
    WHERE banco = 'BANESCO99' AND moneda_ref = 'BS' LIMIT 1
),
SumasPorTiempo AS (
    SELECT mes, semana, SUM(monto_ref) as suma_monto_ref
    FROM banesco99_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%' AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY mes, semana
)
SELECT
    s.mes,
    s.semana,
    s.suma_monto_ref as movimiento_semana,
    COALESCE(si.saldoinicial, 0) + SUM(s.suma_monto_ref) OVER (ORDER BY s.mes, s.semana) as saldo_acumulado
FROM SumasPorTiempo s
CROSS JOIN SaldoInicial si
ORDER BY s.mes, s.semana;

SELECT
    fecha,
    descripcion,
    monto_ref
FROM banesco99_bs_norm
WHERE mes = 11
  AND semana = 4
ORDER BY monto_ref ASC -- Muestra los negativos más grandes primero
LIMIT 10;

-----Scripts de Banesco Panama (FINAL - AÑO, FECHA Y DECIMALES) ------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE banescopanama_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE banescopanama_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    -- Encuentra el saldo inicial específico para BANESCOPANAMA US
    SELECT
        saldoinicial,fecha
    FROM saldos_iniciales
    WHERE banco = 'BANESCOPANAMA'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para tomar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM banescopanama_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular saldo final acumulativo cronológicamente
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE banescopanama_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta usando ::numeric y ROUND
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de BVC (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE bvc_bs_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE bvc_bs_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'BVC'
      AND moneda_ref = 'BS'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM bvc_bs_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular acumulado respetando el orden cronológico (Año -> Mes -> Semana)
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE bvc_bs_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;

----Scripts de Caja (FINAL - AÑO, FECHA Y DECIMALES) ----------------------------

-- Paso 1: Crear columna es_saldo_final si no existe
ALTER TABLE caja_us_norm
ADD COLUMN IF NOT EXISTS es_saldo_final BOOLEAN;

-- Paso 2: Crear columna para el saldo_final_calculado
ALTER TABLE caja_us_norm
ADD COLUMN IF NOT EXISTS saldo_final_calculado DECIMAL(15,2);

-- Paso 3 y 4: Calcular y Actualizar
WITH SaldoInicial AS (
    SELECT
        saldoinicial
    FROM saldos_iniciales
    WHERE banco = 'CAJA'
      AND moneda_ref = 'US'
    -- MEJORA: Ordenamos por fecha para asegurar el punto de partida correcto
    ORDER BY fecha ASC
    LIMIT 1
),
SumasPorTiempo AS (
    -- Agrupamos por AÑO, MES y SEMANA
    SELECT
        ano,
        mes,
        semana,
        SUM(monto_ref) as suma_monto_ref
    FROM caja_us_norm
    WHERE descripcion NOT LIKE '%SALDO FINAL%'
      AND descripcion NOT LIKE '%SALDO INICIAL%'
    GROUP BY ano, mes, semana
),
SaldosAcumulativos AS (
    -- Calcular acumulado respetando el orden cronológico (Año -> Mes -> Semana)
    SELECT
        s.ano,
        s.mes,
        s.semana,

        COALESCE(si.saldoinicial, 0) +
        SUM(s.suma_monto_ref) OVER (
            -- ORDEN VITAL: Año -> Mes -> Semana (Continuidad 2025-2026)
            ORDER BY s.ano ASC, s.mes ASC, s.semana ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as saldo_acumulado_calculado

    FROM SumasPorTiempo s
    CROSS JOIN SaldoInicial si
)
-- Paso Final: Actualización optimizada con JOIN
UPDATE caja_us_norm b
SET
    saldo_final_calculado = sac.saldo_acumulado_calculado,

    -- MEJORA: Comparación exacta con redondeo y casteo numérico
    es_saldo_final = (
        ROUND(sac.saldo_acumulado_calculado::numeric, 2) = ROUND(b.saldo_ref::numeric, 2)
    )
FROM SaldosAcumulativos sac
-- El match requiere las 3 coordenadas temporales
WHERE b.ano = sac.ano
  AND b.mes = sac.mes
  AND b.semana = sac.semana;