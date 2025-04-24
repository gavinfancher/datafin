# DataFin

Welcome!

### AWS Bucket structure
```
my-bucket/
├── archive/  # all sorts of stuff
├── dev/
│   ├── fmp/
│   │   └── forex/
│   │       ├── mini-transform/
│   │       └── raw/
│   └── polygon/
│       ├── equities/
│       ├── mini-transform/
│       │   └── year=2025/
│       │       └── month=04/
│       │           └── mini-transform-2025-04-XX.parquet  # XX represents the day
│       └── raw/
│           └── year=2025/
│               └── month=04/
│                   └── raw-2025-04-XX.parquet             # XX represents the day
└── ref-data/
    ├── forex-pairs.json
    ├── nasdaq100-constituents.json
    ├── snp-nas-constituents-combo.json
    └── snp500-constituents.json
```

