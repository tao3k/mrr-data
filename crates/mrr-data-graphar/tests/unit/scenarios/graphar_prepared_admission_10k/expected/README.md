# Expected

Every measured iteration must consume the same immutable Arrow batches, restore
all 10,000 canonical facts, and perform no native GraphAr storage read or Arrow
C Stream import.
