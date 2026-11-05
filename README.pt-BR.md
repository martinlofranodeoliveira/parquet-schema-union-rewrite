# Parquet Schema Union Rewrite

**Versão de desenvolvimento 0.1.0.** Projeto original da [Lofrano Tecnologia](https://lofranotecnologia.com.br) para pequenos arquivos Parquet **sintéticos e próprios**. Faz união explícita de um subconjunto de esquemas, mantém a ordem dos registros, normaliza dicionários de strings observadas e acrescenta proveniência. PyArrow realiza leitura/escrita real; consultas restritas do DuckDB verificam o resultado antes da publicação.

Não é serviço geral de ingestão, sandbox de segurança, benchmark nem implementação universal do padrão Parquet. Operações da biblioteca, testes e demonstração não usam modelos, APIs pagas, rede, telemetria, instalação de extensões ou dados externos.

## Instalação e testes

Alvo: **CPython 3.13, Linux x64**, PyArrow **26.0.1** e DuckDB **2.0.0**. A qualificação fornecida utilizou CPython 3.13.18. As versões e os hashes reais das rodas estão em `requirements.lock`; consulte o [registro de qualificação](docs/qualification.md). A preparação de dependências é uma etapa separada da execução offline.

Em um diretório com o código-fonte e uma conta sem privilégios:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install --only-binary=:all: --require-hashes -r requirements.lock
python -m pip install --no-index --no-build-isolation --no-deps .
python -m pytest -q
python -m parquet_union_replay demo --assert
python -m parquet_union_replay demo --assert --json
```

Com rodas previamente disponibilizadas:

```sh
python -m pip install --no-index --find-links /opt/wheelhouse --only-binary=:all: --require-hashes -r requirements.lock
```

Os hashes não representam todas as plataformas. Uma roda diferente é rejeitada; não altere os pins apenas para contornar a verificação.

### Contêiner público, sem montar o host

O Dockerfile prepara explicitamente as dependências, gera o sdist, instala uma roda e uma instalação a partir do fonte. Depois usa UID 1000. A preparação pode acessar a rede; as execuções abaixo não podem. Não é necessária imagem privada.

```sh
docker build --platform linux/amd64 -t parquet-union-local .
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m parquet-union-local python -m pytest -q
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --workdir /tmp parquet-union-local python -I -m parquet_union_replay demo --assert --json
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --workdir /tmp --env PYTHONPATH=/opt/source-install parquet-union-local python -m parquet_union_replay demo --assert --json
```

As duas últimas execuções importam instalações fora do checkout. A regressão real de permissão exige execução sem privilégios e falha se os testes forem executados como root. Não há volumes do host, credenciais nem contêiner privilegiado.

## Demonstração sintética determinística

Saída humana esperada:

```text
Synthetic union: rows=4; sum(id)=10; non-null note=1
Observed color dictionary: green, red
Independent verification completed; JSON follows.
```

Em seguida vem um objeto JSON com versões reais do ambiente. `--json` emite somente esse objeto. Os campos semânticos esperados são `rows=4`, `id_sum=10`, `note_nonnull=1`, dicionário `["green","red"]`, grupos green=1/red=2/null=1 e proveniência `[[0,0],[0,1],[1,0],[1,1]]`. Não há horários nem caminhos temporários.

A contém `(1,red)` e `(2,null)`; B contém `(3,green,x)` e `(4,red,null)`. O valor não observado `blue` desaparece do dicionário. A saída tem campos `color,id,note,__source_file,__source_row`; `id` é promovido para int64. Os arquivos fonte usam grupos de uma linha; a saída usa grupos de duas, sem compressão, e exige codificação real de dicionário para `color`. A demonstração remove seus arquivos temporários.

## API pública

```python
from pathlib import Path
from parquet_union_replay import UnionError, plan_union, rewrite, inspect_receipt

manifest = [Path("fixtures/a.parquet"), Path("fixtures/b.parquet")]
plan = plan_union(manifest)
try:
    receipt = rewrite(manifest, "artifacts/union", row_group_size=2)
except UnionError as error:
    print(error.code)
else:
    assert inspect_receipt("artifacts/union") == receipt
```

Os diretórios pais precisam existir. `plan_union` lê bytes e metadados limitados, sem materializar linhas. `rewrite` refaz o plano; ele não aceita um plano antigo como autorização para publicar. Destinos existentes, mesmo vazios, nunca são sobrescritos. As funções compartilham uma instância serializada; `UnionRewriter()` cria outra. Mantenha a área privada e os fontes imutáveis mesmo com instâncias separadas.

## CLI

O manifesto é um array JSON de 1 a 8 caminhos locais explícitos:

```json
["a.parquet", "b.parquet"]
```

Entradas relativas são interpretadas em relação ao arquivo de manifesto. Na API, são relativas ao diretório atual. Repetir um arquivo repete intencionalmente seus registros.

```sh
python -m parquet_union_replay plan fixtures/manifest.json
python -m parquet_union_replay rewrite fixtures/manifest.json artifacts/union --row-group-size 2
python -m parquet_union_replay inspect artifacts/union
```

Comandos de dados bem-sucedidos emitem JSON canônico. Erros emitem apenas código constante em stderr: saída 2 antes da publicação; saída 3 para `E_COMMITTED_IO`, quando o rename já ocorreu e a sincronização posterior do diretório pai falhou. Nesse caso o destino **existe**: inspecione-o, sem supor rollback ou tentar sobrescrever.

## Esquema e limites

- Até 8 ocorrências de arquivos, 4096 linhas totais, 16 campos fonte na união global e 64 grupos fonte; até 16 MiB por entrada.
- Somente bool, int32, int64, string UTF-8 comum e dicionário UTF-8 com índices inteiros no nível superior. Apenas int32→int64 é promoção permitida. Declarações não suportadas falham mesmo sem linhas.
- Campos ordenados pelos bytes UTF-8, seguidos da proveniência int64. Campos ausentes viram nulos tipados. A ordem física de grupos/linhas e dos blocos do manifesto é preservada, sem deduplicação.
- Se qualquer declaração do campo string for dicionário, a saída também será. O domínio contém somente valores não nulos observados, em ordem de bytes UTF-8. Sem normalização Unicode.
- Nomes duplicados, ambiguidades ASCII de caixa, nomes reservados, controles e nomes acima de 128 bytes UTF-8 são rejeitados. Não se aplica case folding Unicode.
- Somente arquivos regulares locais, sem links simbólicos em qualquer componente. URLs, globs, barras invertidas, dois-pontos, controles e componentes `..` são rejeitados.

O [contrato bilíngue](docs/contract.md) contém os limites adicionais de saída/recibo, códigos de falha e detalhes transacionais.

## Projeto e limitações

Política explícita evita unificação permissiva de tipos. O pequeno limite permite um oráculo de linhas em memória. DuckDB verifica todas as linhas e sua ordem, tipos SQL, nulos e somas de proveniência. SQL fixo adicional verifica soma de `id` numérico e grupos de `color` string quando esses nomes exatos existem. Arrow inspeciona separadamente esquema, domínios, grupos e compressão. Os testes usam expectativas literais independentes, inclusive extremos int64.

O recibo contém hashes dos fontes sem caminhos, esquema, dicionários, contagens, hash do artefato e versões reais. `inspect_receipt` verifica consistência do hash, estrutura Arrow, semântica DuckDB e formato da proveniência, mas não autentica o recibo nem comprova os fontes originais.

Contagens e limites de bytes **não** são garantias de RSS, descompressão ou uso de disco. Parsers nativos podem alocar antes de aceitar metadados. Mantenha os limites externos de 1 GiB/2 CPUs e não use dados desconhecidos de terceiros. A saída é admitida em até 128 MiB depois da escrita; o recibo em até 4 MiB. Spill do DuckDB fica desativado. Não há garantia de ordem física das páginas de dicionário, identidade binária entre versões, compatibilidade universal ou recuperação de falhas em todo sistema de arquivos. A publicação exige Linux `renameat2(RENAME_NOREPLACE)` e semântica de diretórios no mesmo sistema de arquivos.

Veja [design](docs/design.md), [contrato e falhas](docs/contract.md), [English](README.md) e [segurança](SECURITY.md). Licença MIT, copyright 2026 Lofrano Tecnologia. Contato geral: [site oficial](https://lofranotecnologia.com.br).
