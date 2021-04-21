SELECT
    nycct.*,
    tai.laborpct,
    case
        when tai.laborpct >= 98 then '98% ~ 100%'
        when tai.laborpct >= 96 then '96% ~ 97%'
        when tai.laborpct >= 94 then '94% ~ 95%'
        when tai.laborpct >= 92 then '92% ~ 94%'
        else '90% ~ 91%'
    end as pctcat
FROM
    dcptransportation.nycct
    JOIN dcptransportation.tai ON tai.tractid = nycct.tractid
WHERE
    tai.laborpct >= 90