BEGIN TRANSACTION;
CREATE TABLE intake_batches (
            batch_id TEXT PRIMARY KEY,
            row_count INTEGER NOT NULL CHECK (row_count > 0)
        );
INSERT INTO "intake_batches" VALUES('early',3);
CREATE TABLE intake_rows (
            batch_id TEXT NOT NULL REFERENCES intake_batches(batch_id),
            position INTEGER NOT NULL,
            observation TEXT NOT NULL,
            partition TEXT NOT NULL,
            source TEXT NOT NULL,
            source_line INTEGER NOT NULL,
            mm TEXT,
            PRIMARY KEY (batch_id, position),
            UNIQUE (batch_id, observation)
        );
INSERT INTO "intake_rows" VALUES('early',1,'R','east','field-sheet',4,'0x7');
INSERT INTO "intake_rows" VALUES('early',2,'S','east','field-sheet',6,'0x7');
INSERT INTO "intake_rows" VALUES('early',3,'B','north','field-sheet',9,'0x5');
CREATE TABLE ledger_book (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            lake_id TEXT NOT NULL,
            day TEXT NOT NULL
        );
INSERT INTO "ledger_book" VALUES(1,'survey-legacy','2025-08-14');
CREATE TABLE official_revisions (
            revision INTEGER PRIMARY KEY,
            parent INTEGER REFERENCES official_revisions(revision),
            operation_kind TEXT NOT NULL CHECK (operation_kind IN ('post', 'correction')),
            operation_id TEXT NOT NULL,
            affected TEXT NOT NULL,
            UNIQUE (operation_kind, operation_id)
        );
INSERT INTO "official_revisions" VALUES(1,NULL,'post','early','["east","north"]');
INSERT INTO "official_revisions" VALUES(2,1,'correction','same-east','["east"]');
INSERT INTO "official_revisions" VALUES(3,2,'correction','same-north','["north"]');
CREATE TABLE official_rows (
            revision INTEGER NOT NULL REFERENCES official_revisions(revision),
            position INTEGER NOT NULL,
            batch_id TEXT NOT NULL,
            observation TEXT NOT NULL,
            partition TEXT NOT NULL,
            source TEXT NOT NULL,
            source_line INTEGER NOT NULL,
            mm TEXT,
            PRIMARY KEY (revision, position),
            UNIQUE (revision, batch_id, observation)
        );
INSERT INTO "official_rows" VALUES(1,1,'early','R','east','field-sheet',4,'0x7');
INSERT INTO "official_rows" VALUES(1,2,'early','S','east','field-sheet',6,'0x7');
INSERT INTO "official_rows" VALUES(1,3,'early','B','north','field-sheet',9,'0x5');
INSERT INTO "official_rows" VALUES(2,1,'early','R','east','field-sheet',4,'0x7');
INSERT INTO "official_rows" VALUES(2,2,'early','S','east','field-sheet',6,'0x7');
INSERT INTO "official_rows" VALUES(2,3,'early','B','north','field-sheet',9,'0x5');
INSERT INTO "official_rows" VALUES(3,1,'early','R','east','field-sheet',4,'0x7');
INSERT INTO "official_rows" VALUES(3,2,'early','S','east','field-sheet',6,'0x7');
INSERT INTO "official_rows" VALUES(3,3,'early','B','north','field-sheet',9,'0x5');
CREATE TABLE posted_batches (
            batch_id TEXT PRIMARY KEY REFERENCES intake_batches(batch_id),
            revision INTEGER NOT NULL UNIQUE REFERENCES official_revisions(revision)
        );
INSERT INTO "posted_batches" VALUES('early',1);
CREATE TABLE release_catalog (
            release_id TEXT PRIMARY KEY,
            revision INTEGER NOT NULL UNIQUE REFERENCES official_revisions(revision)
        );
CREATE TABLE release_parts (
            release_id TEXT NOT NULL REFERENCES release_catalog(release_id),
            partition TEXT NOT NULL,
            payload BLOB NOT NULL,
            PRIMARY KEY (release_id, partition)
        );
COMMIT;
