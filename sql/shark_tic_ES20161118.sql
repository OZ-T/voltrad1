CREATE TABLE shark_tic_ES20161118_dup ( like "shark_tic_ES20161118" ) ;


INSERT INTO shark_tic_ES20161118_dup (
            index, "AD1PCT", "AD1SD1D", "BearCallMaxDShortJL", "BearCallMaxDeltaShort", 
            "BearCallMaxDeltaShortPos", "BearCallMaxIVShort", "BullPutMaxDShortJL", 
            "BullPutMaxDeltaShort", "BullPutMaxDeltaShortPos", "BullPutMaxIVShort", 
            "DTMaxdatost", "DTMaxdatost_1", "DToperaciones", "DshortPosition", 
            "GammaPosition", "ImplVolATM", "MargenNeto", "MaxDs", "MaxDsAD1PCT", 
            "MaxDsAD1SD1D", "PnL", "Pts1SD1D", "Pts1SD5D", "ThetaPosition", 
            "VegaPosition", "VegaThetaRatio", accountid, comisiones, coste_base, 
            dit, dte, dte_inicio_estrategia, e_v, expiry, impacto_cash, "ini_1SD15D", 
            "ini_1SD21D", iv_atm_inicio_strat, iv_subyacente, "lastUndPrice", 
            linea_mercado, "marketValueGross", "marketValuefromPrices", max_profit, 
            multiplier, orders_precio_bruto, pnl_margin_ratio, portfolio, 
            "portfolio_marketValue", portfolio_precio_neto, "portfolio_unrealizedPNL", 
            "prc_ajuste_1SD15D_dn", "prc_ajuste_1SD15D_up", "prc_ajuste_1SD21D_dn", 
            "prc_ajuste_1SD21D_up", precio_close_anterior_subyacente, precio_last_subyacente, 
            precio_undl_inicio_strat, puntos_desde_last_close, retorno_subyacente, 
            subyacente, "thetaDeltaRatio", "thetaGammaRatio", "unrealizedPNLfromPrices")
     ( select             index, "AD1PCT", "AD1SD1D", "BearCallMaxDShortJL", "BearCallMaxDeltaShort", 
            "BearCallMaxDeltaShortPos", "BearCallMaxIVShort", "BullPutMaxDShortJL", 
            "BullPutMaxDeltaShort", "BullPutMaxDeltaShortPos", "BullPutMaxIVShort", 
            "DTMaxdatost", "DTMaxdatost_1", "DToperaciones", "DshortPosition", 
            "GammaPosition", "ImplVolATM", "MargenNeto", "MaxDs", "MaxDsAD1PCT", 
            "MaxDsAD1SD1D", "PnL", "Pts1SD1D", "Pts1SD5D", "ThetaPosition", 
            "VegaPosition", "VegaThetaRatio", accountid, comisiones, coste_base, 
            dit, dte, dte_inicio_estrategia, e_v, expiry, impacto_cash, "ini_1SD15D", 
            "ini_1SD21D", iv_atm_inicio_strat, iv_subyacente, "lastUndPrice", 
            linea_mercado, "marketValueGross", "marketValuefromPrices", max_profit, 
            multiplier, orders_precio_bruto, pnl_margin_ratio, portfolio, 
            "portfolio_marketValue", portfolio_precio_neto, "portfolio_unrealizedPNL", 
            "prc_ajuste_1SD15D_dn", "prc_ajuste_1SD15D_up", "prc_ajuste_1SD21D_dn", 
            "prc_ajuste_1SD21D_up", precio_close_anterior_subyacente, precio_last_subyacente, 
            precio_undl_inicio_strat, puntos_desde_last_close, retorno_subyacente, 
            subyacente, "thetaDeltaRatio", "thetaGammaRatio", "unrealizedPNLfromPrices"
		from "shark_tic_ES20161118"
     
            );




   