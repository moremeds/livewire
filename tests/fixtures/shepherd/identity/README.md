# Shepherd stable-security identity fixtures

These sanitized fixtures freeze the LS-02.2a acceptance contract. They are not
a production security master and do not grant a case permission to publish.

- `sources.json` records only fields observed from bounded source retrievals,
  along with retrieval time, source revision or response identity, exact raw
  response SHA-256, and fields that were absent.
- `cases.json` freezes the collision and transition decisions that LS-02.2b
  must implement. Counterfactual cases test policy without asserting that the
  named placeholder security exists.
- `manifest.json` hashes the two JSON inputs. A change to either source
  inventory or expected disposition is therefore a reviewed contract change.

Provider credentials and full licensed Massive responses are deliberately not
committed. The selected records are sufficient to reproduce the collision
shape while `raw_sha256` binds each record to the response observed during the
inventory. The exact raw bytes belong in the LS-02.1 evidence store during a
live run.

Wikipedia is discovery/secondary evidence. SEC and responsible-publisher
material establish issuer and event facts. Massive supplies useful listed-
security identifiers when the current entitlement returns them, but a missing
field or provider disagreement remains explicit instead of being inferred.
