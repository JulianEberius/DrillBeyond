select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment,--,
                supplier.revenue
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = 9
                and p_type like '%TIN'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'MIDDLE EAST'
                and supplier.revenue > 100.0
                and ps_supplycost = (
                    select
                        min(ps_supplycost)
                    from
                        partsupp,
                        supplier,
                        nation,
                        region
                    where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'MIDDLE EAST'
                        -- and nation.population > 10000000
                )
            order by
                supplier.revenue,
                s_acctbal,
                n_name,
                s_name,
                p_partkey
            limit 100;