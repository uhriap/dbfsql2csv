select
    a.SMO,
    a.PVP, -- нет в шаблоне
    a.P_CODE,
    a.FAM,
    a.IM,
    a.OT,
    a.DR,
    a.SEX,
    a.SS,
    a.POLIS,
    a.D_BP,  -- такого вроде нет в шаблоне. какая-то дата?
    a.D_EP,  -- нет в шаблоне
    a.KNPN,
    a.KULN,
    a.DN,
    a.KN,
    a.AF_KNPN,
    a.AF_KULN,
    a.AF_DN,
    a.AF_KN,
    a.AR_ZIP,  -- никаких zip нет в шаблоне
    a.AF_ZIP,
    -- в шаблоне есть какой-то teln
    a.TEL_ZLR,
    a.TEL_ZLD,
    a.E_MAIL,
    a.TEL_PR,
    a.TEL_PD,
    a.E_MAILP,
    a.GOROD,  -- нет в шаблоне
    a.ULICA as ul,  -- видимо просто ul в шаблоне
    a.AF_GOROD,  -- нет в шаблоне
    a.AF_ULICA as ul_fact,  -- видимо ul_fact в шаблоне
    a.GOROD_PVP,  -- нет в шаблоне
    b.DIAG,
    b.DIAG as mkb_code
    -- в шаблоне есть еще:
    -- vozrast
    -- kvartal
    -- lpu
    -- np
    -- np_fakt
from
    -- {0} и {1} заменит на имя первой и второй таблицы при запуске программы
    `{0}` as a INNER JOIN `{1}` as b ON  
       a.POLIS = b.NMBPOL
WHERE
    b.HEALTHGRUP = 3
