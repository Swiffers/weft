CREATE TABLE IF NOT EXISTS run_search_queue (
            run_id UUID PRIMARY KEY
        );
CREATE TABLE IF NOT EXISTS run_search (
            run_id UUID PRIMARY KEY,
            words TSVECTOR NOT NULL,
            nodes TEXT[] NOT NULL
        );
CREATE INDEX IF NOT EXISTS run_search_words ON run_search USING GIN (words);
CREATE INDEX IF NOT EXISTS run_search_nodes ON run_search USING GIN (nodes);
