-- KOSPI 테이블 생성
CREATE TABLE IF NOT EXISTS kospi_stocks (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    price NUMERIC NOT NULL,
    change_rate NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);
CREATE INDEX IF NOT EXISTS idx_kospi_date ON kospi_stocks(date);
CREATE INDEX IF NOT EXISTS idx_kospi_code ON kospi_stocks(code);

-- KOSDAQ 테이블 생성
CREATE TABLE IF NOT EXISTS kosdaq_stocks (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    price NUMERIC NOT NULL,
    change_rate NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);
CREATE INDEX IF NOT EXISTS idx_kosdaq_date ON kosdaq_stocks(date);
CREATE INDEX IF NOT EXISTS idx_kosdaq_code ON kosdaq_stocks(code);

-- RPC 함수 생성
CREATE OR REPLACE FUNCTION check_table_exists(table_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    exists_val BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = table_name
    ) INTO exists_val;
    
    RETURN exists_val;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER; 