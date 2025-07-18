CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
   from_date date NOT NULL,       -- Начало отчетного периода
   to_date date NOT NULL,         -- Конец отчетного периода
   chapter char(1),               -- Глава баланса
   ledger_account char(10) NOT NULL,  -- Балансовый счет
   characteristic char(1),        -- Характеристика счета
   balance_in_rub numeric(23,8),  -- Входящий остаток (рубли)
   balance_in_val numeric(23,8),  -- Входящий остаток (валюта)
   balance_in_total numeric(23,8),-- Входящий остаток (итого)
   turn_deb_rub numeric(23,8),    -- Дебетовые обороты (рубли)
   turn_deb_val numeric(23,8),    -- Дебетовые обороты (валюта)
   turn_deb_total numeric(23,8),  -- Дебетовые обороты (итого)
   turn_cre_rub numeric(23,8),    -- Кредитовые обороты (рубли)
   turn_cre_val numeric(23,8),    -- Кредитовые обороты (валюта)
   turn_cre_total numeric(23,8),  -- Кредитовые обороты (итого)
   balance_out_rub numeric(23,8), -- Исходящий остаток (рубли)
   balance_out_val numeric(23,8), -- Исходящий остаток (валюта)
   balance_out_total numeric(23,8),-- Исходящий остаток (итого)
   PRIMARY KEY (from_date, to_date, ledger_account)
);
-- ----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_id INTEGER;
    rows_affected INTEGER;
    v_start_time TIMESTAMP;
    v_from_date DATE;
    v_to_date DATE;
    v_prev_date DATE;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Логирование начала операции
    INSERT INTO logs.load_log(table_name, start_time, status) 
    VALUES ('dm_f101_round_f', v_start_time, 'started')
    RETURNING id INTO log_id;
    
    -- Определение отчетного периода
    v_from_date := date_trunc('month', i_OnDate - INTERVAL '1 month')::DATE;
    v_to_date := (date_trunc('month', i_OnDate) - INTERVAL '1 day')::DATE;
    v_prev_date := v_from_date - INTERVAL '1 day';
    
    -- Удаление существующих данных
    DELETE FROM dm.dm_f101_round_f 
    WHERE from_date = v_from_date 
      AND to_date = v_to_date;
    
    -- Расчет и вставка данных с исправленной агрегацией
    INSERT INTO dm.dm_f101_round_f
    WITH account_info AS (
        SELECT DISTINCT
            substring(a.account_number, 1, 5) AS ledger_account,
            a.currency_code,
            a.account_rk,
            las.chapter,
            las.characteristic
        FROM ds.md_account_d a
        JOIN ds.md_ledger_account_s las 
            ON substring(a.account_number, 1, 5) = las.ledger_account::TEXT
            AND v_to_date BETWEEN las.start_date AND COALESCE(las.end_date, '9999-12-31')
        WHERE v_to_date BETWEEN a.data_actual_date AND a.data_actual_end_date
    ),
    balance_data AS (
        SELECT
            ai.ledger_account,
            SUM(CASE 
                WHEN b.on_date = v_prev_date AND ai.currency_code IN ('643', '810') 
                THEN b.balance_out_rub ELSE 0 
            END) AS balance_in_rub,
            SUM(CASE 
                WHEN b.on_date = v_prev_date AND ai.currency_code NOT IN ('643', '810') 
                THEN b.balance_out_rub ELSE 0 
            END) AS balance_in_val,
            SUM(CASE 
                WHEN b.on_date = v_to_date AND ai.currency_code IN ('643', '810') 
                THEN b.balance_out_rub ELSE 0 
            END) AS balance_out_rub,
            SUM(CASE 
                WHEN b.on_date = v_to_date AND ai.currency_code NOT IN ('643', '810') 
                THEN b.balance_out_rub ELSE 0 
            END) AS balance_out_val
        FROM account_info ai
        LEFT JOIN dm.dm_account_balance_f b 
            ON ai.account_rk = b.account_rk
            AND b.on_date IN (v_prev_date, v_to_date)
        GROUP BY ai.ledger_account
    ),
    turnover_data AS (
        SELECT
            ai.ledger_account,
            SUM(CASE 
                WHEN ai.currency_code IN ('643', '810') 
                THEN t.debet_amount_rub ELSE 0 
            END) AS turn_deb_rub,
            SUM(CASE 
                WHEN ai.currency_code NOT IN ('643', '810') 
                THEN t.debet_amount_rub ELSE 0 
            END) AS turn_deb_val,
            SUM(CASE 
                WHEN ai.currency_code IN ('643', '810') 
                THEN t.credit_amount_rub ELSE 0 
            END) AS turn_cre_rub,
            SUM(CASE 
                WHEN ai.currency_code NOT IN ('643', '810') 
                THEN t.credit_amount_rub ELSE 0 
            END) AS turn_cre_val
        FROM account_info ai
        LEFT JOIN dm.dm_account_turnover_f t 
            ON ai.account_rk = t.account_rk
            AND t.on_date BETWEEN v_from_date AND v_to_date
        GROUP BY ai.ledger_account
    ),
    combined_data AS (
        SELECT
            ai.ledger_account,
            MAX(ai.chapter) AS chapter,
            MAX(ai.characteristic) AS characteristic,
            COALESCE(bd.balance_in_rub, 0) AS balance_in_rub,
            COALESCE(bd.balance_in_val, 0) AS balance_in_val,
            COALESCE(bd.balance_in_rub, 0) + COALESCE(bd.balance_in_val, 0) AS balance_in_total,
            COALESCE(td.turn_deb_rub, 0) AS turn_deb_rub,
            COALESCE(td.turn_deb_val, 0) AS turn_deb_val,
            COALESCE(td.turn_deb_rub, 0) + COALESCE(td.turn_deb_val, 0) AS turn_deb_total,
            COALESCE(td.turn_cre_rub, 0) AS turn_cre_rub,
            COALESCE(td.turn_cre_val, 0) AS turn_cre_val,
            COALESCE(td.turn_cre_rub, 0) + COALESCE(td.turn_cre_val, 0) AS turn_cre_total,
            COALESCE(bd.balance_out_rub, 0) AS balance_out_rub,
            COALESCE(bd.balance_out_val, 0) AS balance_out_val,
            COALESCE(bd.balance_out_rub, 0) + COALESCE(bd.balance_out_val, 0) AS balance_out_total
        FROM account_info ai
        LEFT JOIN balance_data bd ON ai.ledger_account = bd.ledger_account
        LEFT JOIN turnover_data td ON ai.ledger_account = td.ledger_account
        GROUP BY 
            ai.ledger_account,
            bd.balance_in_rub,
            bd.balance_in_val,
            bd.balance_out_rub,
            bd.balance_out_val,
            td.turn_deb_rub,
            td.turn_deb_val,
            td.turn_cre_rub,
            td.turn_cre_val
    )
    SELECT
        v_from_date AS from_date,
        v_to_date AS to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    FROM combined_data;
    
    -- Обновление лога
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    UPDATE logs.load_log 
    SET 
        end_time = clock_timestamp(),
        status = 'completed',
        rows_loaded = rows_affected
    WHERE id = log_id;
    
EXCEPTION WHEN OTHERS THEN
    UPDATE logs.load_log 
    SET 
        end_time = clock_timestamp(),
        status = 'error',
        error_message = SQLERRM
    WHERE id = log_id;
    RAISE;
END;
$$;
-- -- TRUNCATE dm.dm_f101_round_f;
CALL  dm.fill_f101_round_f('2018-02-01');
SELECT *
FROM dm.dm_f101_round_f
ORDER BY ledger_account;

