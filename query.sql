select
    a.SMO, -- Char 2
    a.PVP, -- Char 3
    a.GOROD_PVP as GOROD_PV, -- Char 50
    a.P_CODE, -- Int
    '009' as LPU, -- Char 3 в кавычки нужно вписать правильный код
    a.FAM, -- Char 50
    a.IM, -- Char 50
    a.OT, -- Char 50
    a.DR, -- Date
    a.SS,  -- Char 14
    a.SEX, -- Char 1
    a.POLIS, -- Char 16
    a.D_BP,  -- Date 
    a.D_EP,  -- Date
    a.KNPN,  -- Char 3
    a.KULN,  -- Char 3
    a.DN,    -- Char 10
    a.KN,    -- Char 10
    a.GOROD,  -- Char 50
    a.ULICA,  -- Char 50
    a.AR_ZIP,  -- Char 6
    a.AF_KNPN, -- Char 3
    a.AF_KULN, -- Char 3
    a.AF_DN,  -- Char 10
    a.AF_KN,  -- Char 10
    a.AF_GOROD,  -- Char 50
    a.AF_ULICA, -- Char 50
    a.AF_ZIP,  -- Char 6
    a.TEL_ZLR,  -- Char 15
    a.TEL_ZLD,  -- Char 15
    a.E_MAIL,   -- Char 50
    a.TEL_PR,   -- Char 15
    a.TEL_PD,   -- Char 15
    a.E_MAILP,  -- Char 50
    -- Дата выгрузки, -- Date
    -- 0 as ID_M -- Int
    b.DIAG -- Char 15
from
    -- {0} и {1} заменит на имя первой и второй таблицы при запуске программы
    `{0}` as a INNER JOIN `{1}` as b ON  
       a.POLIS = b.NMBPOL
--     a.FAM = b.FAM
WHERE
    b.HEALTHGRUP = 3
