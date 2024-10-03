-- create_artists_table.sql
CREATE TABLE IF NOT EXISTS artists (
    artist_id SERIAL PRIMARY KEY,
    artist_name_raw TEXT,
    artist_name_clean TEXT UNIQUE
);

-- create_songs_table.sql
CREATE TABLE IF NOT EXISTS songs (
    song_id SERIAL PRIMARY KEY,
    song_name_raw TEXT,
    song_name_clean TEXT,
    artist_id INT,
    callsign VARCHAR(10),
    time BIGINT,
    unique_id VARCHAR(8) UNIQUE,  -- Enforce uniqueness on unique_id
    combined TEXT,
    first_play BOOLEAN,
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);