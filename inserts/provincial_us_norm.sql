create table provincial_us_norm
(
    id                    serial
        primary key,
    fecha                 date           not null,
    mes                   integer,
    semana                integer,
    referencia_bancaria   varchar(100),
    descripcion           text           not null,
    monto_ref             numeric(15, 2) not null,
    saldo_ref             numeric(15, 2),
    moneda_ref            varchar(10)    not null,
    tasa_cambio           numeric(15, 4),
    monto_usd             numeric(15, 2) not null,
    concepto_ey           varchar(200),
    proveedor_cliente     varchar(200),
    tipo_operacion        varchar(20)    not null,
    banco                 varchar(50)    not null,
    fecha_carga           timestamp default CURRENT_TIMESTAMP,
    es_saldo_final        boolean,
    saldo_final_calculado numeric(15, 2),
    ano                   integer   default 2025
);

alter table provincial_us_norm
    owner to postgres;

INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (1, '2025-10-13', 10, 3, '2', 'COMPRA M.E.      00000114370', 1000.00, 1000.00, 'US', 1.0000, 1000.00, null, null, 'DEBITO', 'PROVINCIAL', '2025-12-19 15:34:04.342874', true, 1000.00, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (2, '2025-10-21', 10, 4, '3', 'COMPRA M.E.      00000114464', 10000.00, 11000.00, 'US', 1.0000, 10000.00, null, null, 'DEBITO', 'PROVINCIAL', '2025-12-19 15:34:04.342874', false, 121.32, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (3, '2025-10-22', 10, 4, '4', 'RV.COMPRA M.E.  J0500618832 ', -10000.00, 1000.00, 'US', 1.0000, -10000.00, null, 'RV.COMPRA J0500618832', 'DEBITO', 'PROVINCIAL', '2025-12-19 15:34:04.342874', false, 121.32, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (4, '2025-10-23', 10, 4, '5', 'COMPRA M.E.      00000114504', 10000.00, 11000.00, 'US', 1.0000, 10000.00, null, null, 'DEBITO', 'PROVINCIAL', '2025-12-19 15:34:04.342874', false, 121.32, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (5, '2025-11-03', 11, 1, '9', 'COMPRA M.E.      00000114631', 10000.00, 10121.32, 'US', 1.0000, 10000.00, null, null, 'DEBITO', 'PROVINCIAL', '2026-01-21 22:19:55.296066', true, 10121.32, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (7, '2025-10-31', 10, 4, '00000', 'BRECHA ENTRE OCTUBRE Y NOVIEMBRE', -10878.68, 121.32, 'US', 1.0000, -10878.68, null, null, 'NEUTRA', 'PROVINCIAL', '2026-01-22 13:20:03.876565', true, 121.32, 2025);
INSERT INTO public.provincial_us_norm (id, fecha, mes, semana, referencia_bancaria, descripcion, monto_ref, saldo_ref, moneda_ref, tasa_cambio, monto_usd, concepto_ey, proveedor_cliente, tipo_operacion, banco, fecha_carga, es_saldo_final, saldo_final_calculado, ano) VALUES (6, '2025-11-30', 11, 4, '10', 'COMISION COBRADA POR ASESORI', -47.27, 10074.05, 'US', 1.0000, -47.27, null, 'COBRADA ASESORI', 'DEBITO', 'PROVINCIAL', '2026-01-21 22:19:55.296066', true, 10074.05, 2025);
