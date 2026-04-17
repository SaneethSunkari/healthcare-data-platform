# Ingestion

This folder will hold the FHIR parsing, normalization, and ETL code.

Use `./generate_synthea_data.sh` to refresh local fake patient data in `../synthea/output/fhir/`.

Load generated FHIR bundles into PostgreSQL with:

```bash
python3 ingestion/fhir_parser.py --input-dir synthea/output/fhir
```
