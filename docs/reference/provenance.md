# Provenance bundle layout

With cwltool 3.3.20260925135507, `--provenance PATH` writes a CWLProv 0.6.0
Research Object using BagIt packaging. Observed files include:

| Path | Content |
|---|---|
| `bagit.txt`, `bag-info.txt` | Bag metadata |
| `manifest-sha1.txt`, `tagmanifest-*.txt` | Payload and metadata checksums |
| `metadata/manifest.json` | Research Object manifest |
| `workflow/packed.cwl` | Packed workflow |
| `workflow/primary-job.json` | Input record |
| `workflow/primary-output.json` | Output record |
| `metadata/provenance/primary.cwlprov.*` | PROV serializations |
| `metadata/logs/` | Engine logs |
| `data/` | Content-addressed payload |
| `snapshot/` | Captured workflow source |

The layout can evolve with cwltool. A Minikube smoke run verified output creation
and 40 checksum entries; it did not certify every provenance relationship.
The observed bundle did not contain `ro-crate-metadata.json`. Research Object,
CWLProv, and RO-Crate are not interchangeable format claims.

See [collect reports and provenance](../how-to/reporting.md) for execution and
sharing guidance. Resource-usage JSON from `--usage-report` is a separate artifact.
