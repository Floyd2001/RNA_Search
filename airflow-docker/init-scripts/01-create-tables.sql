CREATE TABLE IF NOT EXISTS rna_targets (
    rna_id VARCHAR(255),
    rna_name TEXT,
    rna_type TEXT,
    target_name TEXT,
    disease TEXT,
    confidence_level TEXT,
    PRIMARY KEY (rna_id)
);
