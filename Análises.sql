-- 1. Temperatura Média Mensal/Regional
SELECT 
    t.mes_ano AS Data,
    r.nome_regiao AS Regiao,
    AVG(c.Temperatura_Media) AS Temperatura_Media
FROM Estatisticas_Regionais er
JOIN Clima c ON er.ID_Clima = c.ID_Clima
JOIN Tempo t ON er.ID_Tempo = t.ID_Tempo
JOIN Regiao r ON er.ID_Regiao = r.ID_Regiao
GROUP BY t.mes_ano, r.nome_regiao
ORDER BY t.mes_ano, r.nome_regiao;

-- 2. Evapotranspiração Média Mensal/Regional
SELECT 
    t.mes_ano AS Data,
    r.nome_regiao AS Regiao,
    AVG(c.Evapotranspiracao_Media) AS Evapotranspiracao_Media
FROM Estatisticas_Regionais er
JOIN Clima c ON er.ID_Clima = c.ID_Clima
JOIN Tempo t ON er.ID_Tempo = t.ID_Tempo
JOIN Regiao r ON er.ID_Regiao = r.ID_Regiao
GROUP BY t.mes_ano, r.nome_regiao
ORDER BY t.mes_ano, r.nome_regiao;

-- 3. Precipitação Média Mensal/Regional
SELECT 
    t.mes_ano AS Data,
    r.nome_regiao AS Regiao,
    AVG(p.Precipitacao_Media) AS Precipitacao_Media
FROM Estatisticas_Regionais er
JOIN Precipitacao p ON er.ID_Clima = p.ID_Precipitacao
JOIN Tempo t ON er.ID_Tempo = t.ID_Tempo
JOIN Regiao r ON er.ID_Regiao = r.ID_Regiao
GROUP BY t.mes_ano, r.nome_regiao
ORDER BY t.mes_ano, r.nome_regiao;

-- 4. Correlação Entre Temperatura e Evapotranspiração
SELECT 
    r.nome_regiao AS Regiao,
    CORR(c.Temperatura_Media, c.Evapotranspiracao_Media) AS Correlacao_Temp_Evap
FROM Estatisticas_Regionais er
JOIN Clima c ON er.ID_Clima = c.ID_Clima
JOIN Regiao r ON er.ID_Regiao = r.ID_Regiao
GROUP BY r.nome_regiao
ORDER BY r.nome_regiao;

-- 5. Tendência Temporal: Regressão Linear
SELECT 
    r.nome_regiao AS Regiao,
    REGR_SLOPE(c.Temperatura_Media, EXTRACT(MONTH FROM t.mes_ano)) AS Slope_Temperatura,
    REGR_SLOPE(c.Evapotranspiracao_Media, EXTRACT(MONTH FROM t.mes_ano)) AS Slope_Evapotranspiracao
FROM Estatisticas_Regionais er
JOIN Clima c ON er.ID_Clima = c.ID_Clima
JOIN Tempo t ON er.ID_Tempo = t.ID_Tempo
JOIN Regiao r ON er.ID_Regiao = r.ID_Regiao
GROUP BY r.nome_regiao
ORDER BY r.nome_regiao;

-- 6. Distribuição Geográfica (Mapas de Calor)
SELECT 
    c.Lat AS Latitude,
    c.Lon AS Longitude,
    AVG(c.Temperatura_Media) AS Temperatura_Media,
    AVG(c.Evapotranspiracao_Media) AS Evapotranspiracao_Media,
    AVG(p.Precipitacao_Media) AS Precipitacao_Media
FROM Estatisticas_Regionais er
JOIN Clima c ON er.ID_Clima = c.ID_Clima
JOIN Precipitacao p ON er.ID_Clima = p.ID_Precipitacao
GROUP BY c.Lat, c.Lon
ORDER BY c.Lat, c.Lon;

-- 7. Dados Anômalos (Opcional)
SELECT 
    c.ID_Clima,
    c.Data_Registro,
    c.Temperatura_Media,
    c.Evapotranspiracao_Media,
    p.Precipitacao_Media
FROM Clima c
LEFT JOIN Precipitacao p ON c.ID_Clima = p.ID_Precipitacao
WHERE c.Temperatura_Media > (
    SELECT AVG(Temperatura_Media) + 3 * STDDEV(Temperatura_Media) FROM Clima
)
   OR c.Evapotranspiracao_Media > (
    SELECT AVG(Evapotranspiracao_Media) + 3 * STDDEV(Evapotranspiracao_Media) FROM Clima
)
   OR p.Precipitacao_Media > (
    SELECT AVG(Precipitacao_Media) + 3 * STDDEV(Precipitacao_Media) FROM Precipitacao
);
