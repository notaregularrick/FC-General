create materialized view detalle_cla as
select distinct concepto_ey, tipo_de_op
    from ejmclasificaciones;


select clasificacion,sum(monto_usd)
    from balance_general bg
group by clasificacion

select coalesce(d.tipo_de_op,'05. Otros') as concepto_gen,sum(monto_usd)
    from balance_general bg left join detalle_cla d
        on bg.clasificacion=d.concepto_ey
group by tipo_de_op