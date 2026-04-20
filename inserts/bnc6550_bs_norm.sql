create table bnc6550_bs_norm
(
    id                    serial
        primary key,
    fecha                 date,
    mes                   integer,
    semana                integer,
    ano                   integer,
    referencia_bancaria   text,
    descripcion           text,
    monto_ref             numeric(20, 2),
    saldo_ref             numeric(20, 2),
    moneda_ref            varchar(10),
    tasa_cambio           numeric(20, 4),
    monto_usd             numeric(20, 2),
    concepto_ey           text,
    proveedor_cliente     text,
    tipo_operacion        varchar(50),
    banco                 varchar(50),
    fecha_carga           timestamp,
    es_saldo_final        boolean,
    saldo_final_calculado numeric(20, 2)
);

alter table bnc6550_bs_norm
    owner to postgres;

INSERT INTO public.bnc6550_bs_norm (id, fecha, mes, semana, ano, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado) VALUES (1, '2025-10-31', 10, 4, 2025, '0', 'Comisión Mantenimiento Cuenta Mes: Octubre   - 2025', -8.00, 2514.72, 'BS', 223.6459, -0.04, null, null, 'DEBITO', 'BNC6550', '2026-03-26 09:54:39.950175', false, 2514.72);
INSERT INTO public.bnc6550_bs_norm (id, fecha, mes, semana, ano, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado) VALUES (2, '2025-10-31', 10, 4, 2025, '16321625929391', 'TELF.:000000000000 CTA.: 01340041510411019900 BANCO:0134 TRASPASO ENTRE CUENTAS EMISOR : ALIMENTOS SERIMAR ,C.A', 500000.00, 502514.72, 'BS', 223.6459, 2235.68, null, null, 'CREDITO', 'BNC6550', '2026-03-26 09:54:39.950175', false, 2514.72);
INSERT INTO public.bnc6550_bs_norm (id, fecha, mes, semana, ano, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado) VALUES (3, '2025-10-31', 10, 4, 2025, '195656377', 'TRANSFERENCIA A FAVOR DE: ALIMENTOS SERIMAR C.A PARA LA CUENTA NRO. 01910181952100096030 TRASPASO ENTRE CUENTAS', -500000.00, 2514.72, 'BS', 223.6459, -2235.68, null, null, 'DEBITO', 'BNC6550', '2026-03-26 09:54:39.950175', true, 2514.72);
INSERT INTO public.bnc6550_bs_norm (id, fecha, mes, semana, ano, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado) VALUES (4, '2025-11-30', 11, 4, 2025, '0', 'Comisión Mantenimiento Cuenta Mes: Noviembre - 2025', -8.00, 2506.72, 'BS', 245.6697, -0.03, null, null, 'DEBITO', 'BNC6550', '2026-03-26 09:54:40.007688', true, 2506.72);
INSERT INTO public.bnc6550_bs_norm (id, fecha, mes, semana, ano, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado) VALUES (5, '2025-12-31', 12, 4, 2025, '0', 'Comisión Mantenimiento Cuenta Mes: Diciembre - 2025', -8.00, 2498.72, 'BS', 298.1431, -0.03, null, null, 'DEBITO', 'BNC6550', '2026-03-26 09:54:40.039567', true, 2498.72);
