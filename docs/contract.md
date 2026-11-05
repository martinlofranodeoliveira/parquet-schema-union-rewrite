# Development 0.1.0 contract / Contrato de desenvolvimento 0.1.0

The public API is `UnionRewriter`, `plan_union`, `rewrite`, `inspect_receipt`, and `UnionError`. Module functions share a single instance. Each instance serializes its own calls with a lock; there is no process-wide cross-instance scheduler. Return values are ordinary JSON-compatible dictionaries owned by the caller. Exceptions are `UnionError` with a constant `.code`, message and argument; native messages are suppressed. Process termination, native aborts and OS kills cannot be converted into Python errors.

A API pública é composta pelos nomes acima. Cada instância serializa suas chamadas; instâncias distintas não compartilham um agendador. Os retornos são dicionários compatíveis com JSON. Exceções possuem somente código constante. Encerramentos do processo ou falhas nativas fatais não podem ser convertidos em exceções Python.

## Admission / Admissão

| Item | Exact policy / Política exata |
| --- | --- |
| Runtime | CPython 3.13.x, Linux x64; PyArrow 26.0.1, DuckDB 2.0.0; otherwise `E_RUNTIME` |
| Manifest API | non-empty list or tuple, maximum 8 occurrences; strings or path-like local paths |
| Manifest CLI | JSON array, at most 64 KiB; relative paths relative to manifest parent |
| Input bytes | each regular file at most 16,777,216 bytes before parsing |
| Rows | at most 4096 summed across occurrences, including repeated paths |
| Source fields | at most 16 per file **and in the global union**, excluding added provenance |
| Source groups | at most 64 summed across occurrences; an empty file's actual group counts |
| Output group size | integer 1–4096, excluding bool; default/demo 2 |
| Output bytes | at most 134,217,728, checked after writing, before publication |
| Receipt bytes | at most 4,194,304 canonical UTF-8 bytes |
| Names | 1–128 UTF-8 bytes; no Unicode Cc controls, duplicate names or ASCII-case ambiguities |
| Paths | at most 4096 UTF-8 bytes; no Cc controls, `*?[]`, backslash, colon, `..` components, or root-only path |

Path restrictions intentionally exclude URL schemes, glob expansion and traversal. Absolute local paths are accepted. All ancestors must already be real directories without symlinks. Final inputs must be regular non-followed files. A stat occurs before opening; `O_NONBLOCK` and `O_NOFOLLOW` prevent FIFO blocking and link following. Input bytes are copied through bounded local handles, then parsed through Arrow BufferReader, not URI inference. Workspace permissions and immutability are caller responsibilities; this is not a defense against an attacker concurrently replacing directory trees.

Restrições de caminhos excluem URLs, expansão de globs e travessia. Caminhos absolutos locais são aceitos. Ancestrais precisam existir e não podem ser links. Entradas são arquivos regulares abertos sem seguir links e sem bloquear FIFOs. Os bytes são limitados antes de serem enviados ao Arrow. A área privada e sua imutabilidade são responsabilidade de quem chama; não é sandbox contra troca concorrente de diretórios.

## Schema / Esquema

Supported declared Arrow types are bool, signed int32, signed int64, ordinary `string`, and dictionary with integer indices and ordinary `string` values. Dictionary index width/order is not preserved. Unsigned logical columns, null type, large strings, binary, floats, decimals, temporal and nested types are rejected, even in empty files. Source metadata is discarded. No permissive `unify_schemas` policy is used.

Union fields have `nullable=true`. All occurrences of the same exact name must have the same logical type, except int32 and int64 unify to int64. Plain and dictionary string declarations share the same string domain. If any occurrence is dictionary-backed, output is dictionary<int32,string,ordered=false>; otherwise it is plain string. Reads decode dictionaries logically and use safe Arrow casts only to the admitted target type. Missing fields become typed nulls.

Source names are sorted by their exact UTF-8 bytes. Two appended fields, `__source_file` and `__source_row`, are non-nullable signed int64. Reserved-name matching and ambiguity rejection use **ASCII A–Z→a–z only**, globally across files. `A` and `a` conflict; `Á` and `á` do not. There is no Unicode case folding, locale ordering or NFC normalization. Names containing ordinary punctuation and spaces are supported because SQL does not interpolate them.

Tipos declarados suportados: bool, int32/int64 com sinal, string UTF-8 comum e dicionário de strings comuns indexado por inteiros. Tipos não suportados falham inclusive em arquivos vazios. Somente int32→int64 pode promover tipos; string simples e dicionário usam o mesmo domínio lógico. Se qualquer ocorrência for dicionário, a saída usa índices int32. Campos fonte são anuláveis; metadados fonte são descartados. Ordenação usa bytes UTF-8 exatos. Proveniência é int64 não anulável. Colisões usam apenas conversão ASCII de caixa, sem normalização Unicode.

## Rows and dictionaries / Linhas e dicionários

Manifest occurrence order, then physical row-group order, then row order is preserved. No sorting, joins or deduplication. File indices are zero-based manifest occurrence indices. Row indices start at zero for each occurrence and continue across row groups. Empty files contribute schema but no provenance rows.

For every output dictionary column, take the set of observed non-null logical strings across all records and sort it by exact UTF-8 bytes. Null is never a dictionary entry. Unused source entries disappear. An all-null dictionary has an empty domain. Values, including control characters within values, are retained exactly; name restrictions do not alter values.

A ordem é do manifesto, seguida da ordem física de grupos e linhas. Não há joins, ordenação de registros nem deduplicação. Índices começam em zero por ocorrência; arquivos vazios só contribuem esquema. O domínio do dicionário contém apenas strings não nulas observadas em ordem de bytes; nulo nunca é entrada. Valores não são normalizados ou censurados.

## Verification and receipt / Verificação e recibo

`plan_union(manifest)` reads source bytes and metadata but not row data. It returns keys `schema`, `dictionary_columns`, `sources`, `rows`, `row_groups`. Each source record has only index, byte length, SHA-256, row count and group count, not a path. A schema field has name, logical type, dictionary flag and nullable flag. Planning success does not guarantee data pages are readable or a destination writable.

`rewrite(manifest, destination, *, row_group_size=2)` replans, materializes admitted rows, constructs typed arrays, stages a directory in the destination parent, and writes uncompressed Parquet with stored Arrow schema. It requires actual RLE_DICTIONARY encoding somewhere for each non-empty dictionary domain. Physical dictionary page order and version-independent bytes are not promised. Arrow inspects output schema, dictionaries, row counts, row groups and compression. DuckDB checks exact tuples in order, names, SQL types, all-column non-null counts and provenance sums. Fixed additional queries check sum(id) as HUGEINT when `id` is numeric, and groups of `color` when it is string. Integer arithmetic never passes through float. DuckDB connections are in-memory, single-threaded, limited to 256 MB, with empty temp_directory (no spill), installation/autoload/unsigned extensions off, only the candidate path allowed, and external access disabled after installing that path exception.

The committed directory contains `data.parquet`, canonical `receipt.json`, and a fixed `.union-owned` marker. Receipt format is `parquet-union-replay/1` with development_version `0.1.0`; it contains the plan, exact dictionary values, verified metrics, artifact hash/size/layout facts and actual Python/PyArrow/DuckDB/package versions. JSON uses UTF-8, sorted keys, compact separators and a terminal newline. No clocks, random stage names or absolute paths enter it. Hashes identify actual bytes; they do not imply cross-version binary reproducibility.

`inspect_receipt(artifact)` requires a canonical, structurally valid receipt and marker. It checks the data hash and size, repeats Arrow/DuckDB verification, compares metrics and checks provenance blocks against claimed source row counts. Original source files are not reopened. A receipt is not a signature; changing both file and receipt can create a different consistent artifact. Inspection cannot prove original-source ownership, source schema or original row values without those sources.

`plan_union` não decodifica páginas de dados e não garante que a reescrita terá sucesso. `rewrite` refaz o plano e verifica linhas exatas, ordem, nomes, tipos, nulos, agregados e estrutura antes de publicar. DuckDB tem acesso restrito somente ao candidato, sem instalação/autoload de extensões ou spill. O diretório final inclui dados, recibo canônico e marcador fixo. O recibo registra versões reais e hashes de bytes, não identidade binária entre versões. `inspect_receipt` verifica consistência sem reabrir os fontes; não autentica o recibo nem comprova valores ou propriedade dos arquivos originais.

## Publication and failure / Publicação e falha

The parent must exist. An existing destination of any kind, including an empty directory or symlink, fails. The implementation uses Linux `renameat2(RENAME_NOREPLACE)` with no unsafe fallback. Readers and writers close before publication; receipt and data files and the stage directory are fsynced. Input hashes are rechecked immediately before rename. External concurrent mutation remains outside the supported private-workspace contract.

Before rename, any limit, parser, cast, permission, write, oracle, hash recheck or receipt failure leaves the destination absent unless another actor created it. Owned marked staging is cleaned using only known regular files, never recursive deletion. Cleanup failures or abrupt termination may leave a private marked stage; unknown/unmarked directories are not removed. No automatic crash scavenging exists. After successful rename, failure to fsync the parent is `E_COMMITTED_IO`: the artifact is already published and is not deleted. Atomic namespace publication is distinct from a promise of power-loss durability on arbitrary filesystems.

O pai deve existir. Qualquer destino existente falha. A publicação usa rename atômico sem substituição; não há fallback com corrida. Fontes são rechecados por hash antes do rename; mutação externa concorrente continua fora do contrato. Falhas anteriores deixam o destino ausente, salvo criação por outro ator. A limpeza só remove arquivos conhecidos em staging próprio marcado. Pode restar staging após falha de limpeza ou encerramento abrupto. Após rename, `E_COMMITTED_IO` significa artefato já publicado, sem rollback. Atomicidade de nomes não implica durabilidade universal após queda de energia.

## Stable codes / Códigos estáveis

| Code | Meaning / Significado |
| --- | --- |
| E_ARGUMENT | invalid manifest shape, CLI arguments or group size / formato ou argumentos inválidos |
| E_PATH | prohibited path syntax or ancestor / caminho ou ancestral proibido |
| E_INPUT_TYPE | final input is not a regular non-followed file / entrada não regular |
| E_PERMISSION | actual filesystem permission denial outside writer / permissão negada |
| E_IO | other local I/O failure / outra falha local de I/O |
| E_SOURCE_CHANGED | changed source bytes/identity during admission or recheck / fonte alterado |
| E_PARQUET_READ | real Arrow metadata/data parse failure / falha real de leitura Arrow |
| E_SCHEMA | unsupported type or logical conflict / tipo não suportado ou conflito |
| E_NAME, E_NAME_COLLISION, E_RESERVED | invalid, duplicate/ambiguous or reserved names / nomes inválidos, ambíguos ou reservados |
| E_LIMIT_FILES, E_LIMIT_BYTES, E_LIMIT_ROWS, E_LIMIT_COLUMNS, E_LIMIT_GROUPS | admission ceiling exceeded / limite excedido |
| E_CAST | safe Arrow conversion failed / conversão segura falhou |
| E_WRITE | Parquet write/flush/fsync failed, possibly partial / escrita possivelmente parcial falhou |
| E_ORACLE | Arrow layout or DuckDB query/check failed / verificação falhou |
| E_RECEIPT | receipt, hash, marker or provenance inconsistency / recibo inconsistente |
| E_DEST_EXISTS | destination exists, including publication race / destino existente |
| E_PLATFORM | no supported atomic no-replace rename / plataforma sem rename exigido |
| E_COMMITTED_IO | rename succeeded; parent synchronization failed / publicado, sincronização posterior falhou |
| E_RUNTIME | runtime outside exact backend/ABI policy / ambiente fora da política |
| E_RESOURCE | Python MemoryError / falta de memória detectada pelo Python |
| E_INTERNAL | unexpected exception, sanitized / exceção inesperada sanitizada |
| E_INTERRUPTED | CLI interrupted / CLI interrompida |
| E_DEMO_CHECK | literal synthetic demo check failed / expectativa sintética falhou |

Error precedence is not a promise when multiple faults coexist. No error includes source paths, values, SQL or raw Arrow/DuckDB text. CLI success is exit 0; failure is exit 2 except committed-I/O exit 3. Help/version are ordinary successful informational exits. Error codes are not a guarantee that native libraries cannot terminate the process.

Quando há várias falhas, a precedência de códigos não é garantida. Nenhum erro inclui caminhos, valores ou texto nativo. Limites de bytes/linhas/grupos são admissões lógicas, não limites físicos de RSS/disco ou de alocação antes dos metadados; mantenha isolamento externo e entradas sintéticas próprias.

Schema admission uses the logical Arrow schema returned by the maintained Parquet reader. Numeric dictionary page encoding does not create a separate logical type: a producer-side dictionary of int32 values that reads back as int32 is admitted as ordinary int32. Only string dictionary types preserved by the reader participate in logical vocabulary normalization. Row and row-group counts are checked before iterating per-group metadata or reading rows.
