UPDATE balance_general AS bg
SET
    tasa_cambio = tc.venta,
    monto_usd = bg.monto_ref / tc.venta
FROM tc_2025 AS tc
WHERE bg.fecha = tc.fecha
  AND bg.moneda_ref = 'BS';

UPDATE balance_general AS bg
SET
    tasa_cambio = 1,
    monto_usd = bg.monto_ref
WHERE bg.moneda_ref = 'US';

ALTER TABLE resumen_saldos
ADD COLUMN IF NOT EXISTS tasa_mes  numeric(15, 2);
ALTER TABLE resumen_saldos
ADD COLUMN IF NOT EXISTS fecha     date;
ALTER TABLE resumen_saldos
ADD COLUMN IF NOT EXISTS saldo_usd numeric(15, 2);

ALTER TABLE saldos_iniciales
ADD COLUMN IF NOT EXISTS tasa_mes  numeric(15, 2);
ALTER TABLE saldos_iniciales
ADD COLUMN IF NOT EXISTS fecha     date;
ALTER TABLE saldos_iniciales
ADD COLUMN IF NOT EXISTS saldo_usd numeric(15, 2);

UPDATE resumen_saldos AS bg
SET
    tasa_mes = tc.venta,
    saldo_usd = bg.saldofinal / tc.venta
FROM tc_2025 AS tc
WHERE bg.fecha = tc.fecha
  AND bg.moneda_ref = 'BS';

UPDATE resumen_saldos AS bg
SET
    tasa_mes = 1,
    saldo_usd = bg.saldofinal
WHERE bg.moneda_ref = 'US';

UPDATE saldos_iniciales AS bg
SET
    tasa_mes = tc.venta,
    saldo_usd = bg.saldoinicial / tc.venta
FROM tc_2025 AS tc
WHERE bg.fecha = tc.fecha
  AND bg.moneda_ref = 'BS';

UPDATE saldos_iniciales AS bg
SET
    tasa_mes = 1,
    saldo_usd = bg.saldoinicial
WHERE bg.moneda_ref = 'US';