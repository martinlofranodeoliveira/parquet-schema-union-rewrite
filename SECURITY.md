# Security policy

Parquet Schema Union Rewrite is development version 0.1.0 for small, owned synthetic fixtures. It is not a sandbox for untrusted datasets. Native Arrow/DuckDB parsers can allocate before admission checks; counters are not RSS or disk quotas. Use a private immutable workspace and the documented non-root, network-none, capability-dropped container limits. Avoid private, personal or production data.

## Reporting

Use GitHub private vulnerability reporting:

https://github.com/martinlofranodeoliveira/parquet-schema-union-rewrite/security/advisories/new

Do not include real personal data, credentials or private datasets. A minimal synthetic reproduction, dependency versions, platform, error code and whether the destination appeared are useful. Do not post sensitive exploit details in public issues. No response-time SLA is offered. For general contact, use https://lofranotecnologia.com.br.

## Boundaries

Path filtering, non-following opens, extension safeguards and constant errors reduce accidental exposure; they do not defend against a hostile process changing the caller's directories, an administrator, a native-library vulnerability or an OS kill. Receipts are consistency records, not signed attestations. Artifact inspection cannot authenticate source ownership. A committed-I/O error means rename already succeeded and the artifact must not be treated as rolled back.

Only the documented 0.1.0 development contract is covered here. There is no claim of production readiness, broad Parquet compliance or certified analytical correctness.

## Português

Relate vulnerabilidades de forma privada pelo endereço GitHub acima. Use apenas reproduções sintéticas e não envie dados pessoais, credenciais ou bases privadas. Não há SLA de resposta. A ferramenta não é sandbox para arquivos desconhecidos: parsers nativos podem alocar antes dos limites. Mantenha área privada e imutável e os limites externos documentados. Recibos não são assinaturas. `E_COMMITTED_IO` indica publicação já realizada. Contato geral pelo site oficial.
