azure-medallion-pipeline/
├── README.md
├── docs/
│   ├── architecture.png
│   └── screenshots/          # ADF pipeline, Databricks, resultado Synapse
├── infrastructure/           # Bicep/Terraform o setup.azcli
├── data-factory/             # export nativo (pipeline/, linkedService/, dataset/)
├── databricks/
│   └── notebooks/            # bronce → plata (https://ingbarcelli.github.io/Adventure-Works/dbx/Silver_layer.html)
├── synapse/
│   └── sql/                  # OPENROWSET, CREATE EXTERNAL TABLE, CETAS
└── data/
    └── sample/               # 50-100 filas, solo para contexto
