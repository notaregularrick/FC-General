---- Script Resumen de Saldos (FINAL - SOPORTE AÑO Y NO DUPLICADOS) ----

-- Paso 1: Re-crear la tabla con la columna 'ano'
DROP TABLE IF EXISTS resumen_saldos;

CREATE TABLE resumen_saldos (
    ano INT NOT NULL,   -- Nueva columna vital
    mes INT NOT NULL,
    semana INT NOT NULL,
    banco VARCHAR(50) NOT NULL,
    referencia_bancaria VARCHAR(100) NOT NULL,
    moneda_ref VARCHAR(10),
    saldofinal DECIMAL(15, 2),
    -- La llave primaria ahora incluye el año para evitar colisiones entre 2025 y 2026
    CONSTRAINT pk_resumen_saldos PRIMARY KEY (ano, mes, semana, banco, referencia_bancaria)
);

-- Paso 2: Ingesta de datos con la columna 'ano'
INSERT INTO resumen_saldos (ano, mes, semana, banco, referencia_bancaria, moneda_ref, saldofinal)
SELECT DISTINCT ON (ano, mes, semana, banco)
    ano, mes, semana, banco, referencia_bancaria, moneda_ref, saldo_ref
FROM (
    -- 1. PROVINCIAL BS
    SELECT
        ano, mes, semana,
        'PROVINCIAL' as banco,
        COALESCE(referencia_bancaria::VARCHAR, 'S/N') as referencia_bancaria,
        moneda_ref,
        saldo_ref
    FROM provincial_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo final%'

    UNION ALL

    -- 2. MERCANTIL BS
    SELECT
        ano, mes, semana,
        'MERCANTIL',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM mercantil_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo final%'

    UNION ALL

    -- 3. BANPLUS BS
    SELECT
        ano, mes, semana,
        'BANPLUS',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM banplus_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 4. BNC BS
    SELECT
        ano, mes, semana,
        'BNC',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bnc_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 5. BDV BS
    SELECT
        ano, mes, semana,
        'BDV',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bdv_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 6. BANCAMIGA BS
    SELECT
        ano, mes, semana,
        'BANCAMIGA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bancamiga_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 7. BANCAMIGA US
    SELECT
        ano, mes, semana,
        'BANCAMIGA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bancamiga_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 8. MERCANTIL PANAMA
    SELECT
        ano, mes, semana,
        'MERCANTILPANAMA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM mercantilpanama_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 9. MERCANTIL US
    SELECT
        ano, mes, semana,
        'MERCANTIL',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM mercantil_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 10. PROVINCIAL US
    SELECT
        ano, mes, semana,
        'PROVINCIAL',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM provincial_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 11. BANESCO PLANTA
    SELECT
        ano, mes, semana,
        'BANESCOPLANTA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM banescoplanta_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 12. BANESCO VERDE
    SELECT
        ano, mes, semana,
        'BANESCOVERDE',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM banescoverde_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 13. BANESCO 99
    SELECT
        ano, mes, semana,
        'BANESCO99',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM banesco99_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 14. BANESCO PANAMA
    SELECT
        ano, mes, semana,
        'BANESCOPANAMA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM banescopanama_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 15. BVC
    SELECT
        ano, mes, semana,
        'BVC',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bvc_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 16. CAJA
    SELECT
        ano, mes, semana,
        'CAJA',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM caja_us_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'

    UNION ALL

    -- 16. BNC6550
    SELECT
        ano, mes, semana,
        'BNC6550',
        COALESCE(referencia_bancaria::VARCHAR, 'S/N'),
        moneda_ref,
        saldo_ref
    FROM bnc6550_bs_norm
    WHERE es_saldo_final = TRUE
      AND descripcion NOT ILIKE '%Saldo Final%'
) AS combined_data

-- Manejo de duplicados (Upsert)
-- Si la combinación (Año, Mes, Semana, Banco, Referencia) ya existe, actualizamos el saldo.
ON CONFLICT (ano, mes, semana, banco, referencia_bancaria)
DO UPDATE SET
    saldofinal = EXCLUDED.saldofinal,
    moneda_ref = EXCLUDED.moneda_ref;