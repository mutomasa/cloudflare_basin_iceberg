# cloudflare_basin_iceberg

Cloudflare Workers で受けたリクエストをイベントとして Cloudflare Pipelines に送り、R2 上の Apache Iceberg テーブルに貯めて、Basin SQL で集計するサンプルです。

```
クライアント → Worker → Stream → Pipeline → Sink → R2 (Iceberg テーブル) ← Basin SQL
```

- **Worker**: どのリクエストにも `Hello World!` を返し、リクエストの情報(時刻・メソッド・パス・User-Agent・国)をイベントとして Stream に送ります。
- **Stream / Pipeline / Sink**: イベントを受け取り、R2 Data Catalog 上の Iceberg テーブル `default.request_events` に parquet で書き出します。
- **Basin SQL**: テーブルを SQL で集計します。

構成の詳しい説明や図は [architecture_flow.md](architecture_flow.md) を参照してください。

## 必要なもの

- Node.js と npm
- Cloudflare アカウント(**Workers Paid プラン**。Pipelines は無料プランでは使えません)
- `wrangler login` 済みであること

## ファイル構成

| ファイル | 内容 |
| --- | --- |
| `src/index.js` | Worker 本体。レスポンスを返し、`ctx.waitUntil` でイベントを Stream に送る |
| `wrangler.jsonc` | Worker の設定。`env.STREAM` の binding(Stream ID)を持つ |
| `stream-schema.json` | Stream のスキーマ(string の5列) |
| `test/index.spec.js` | Vitest のテスト |
| `architecture_flow.md` | アーキテクチャ・フロー図と、各部品の解説 |
| `todo.md` | 作業の進み具合 |

## セットアップ

以下の名前は例です。名前に使えるのは英数字とアンダースコアだけです(ハイフンは使えません)。

### 1. 依存パッケージのインストール

```sh
npm install
```

### 2. R2 バケットを作り、Data Catalog を有効にする

```sh
npx wrangler r2 bucket create <バケット名>
npx wrangler r2 bucket catalog enable <バケット名>
```

`catalog enable` の出力に表示される **Warehouse** 名は、あとで SQL を実行するときに使います。

### 3. Catalog トークンを作る

ダッシュボードの **R2 → Manage API Tokens** で、**アカウント API トークン**を権限 **Admin Read & Write** で作ります。トークンの値はプロジェクト直下の `.catalog_token` に保存します(`.gitignore` 済みです)。

### 4. Stream・Sink・Pipeline を作る

```sh
# Stream(イベントの受け口)
npx wrangler pipelines streams create request_events_v2 --schema-file stream-schema.json

# Sink(Iceberg テーブルへの書き出し先)
npx wrangler pipelines sinks create events_sink \
  --type r2-data-catalog --bucket <バケット名> \
  --namespace default --table request_events \
  --catalog-token "$(cat .catalog_token)"

# Pipeline(Stream から Sink へ流す SQL)
npx wrangler pipelines create request_events_v2_iceberg \
  --sql "INSERT INTO events_sink SELECT timestamp, method, path, user_agent, country FROM request_events_v2"
```

### 5. Stream ID を `wrangler.jsonc` に設定する

Stream の作成時に表示された ID(`npx wrangler pipelines streams list` でも確認できます)を `wrangler.jsonc` に書きます。

```jsonc
"pipelines": [
	{
		"stream": "<Stream ID>",
		"binding": "STREAM",
	},
],
```

## 開発とデプロイ

```sh
npm run dev      # http://localhost:8787/ でローカル実行
npm test         # テスト(ウォッチモード。1回だけなら npx vitest run)
npm run deploy   # Cloudflare にデプロイ
```

`npm run dev` では `env.STREAM` はローカルのモックになり、本物の Stream には送られません。R2 まで流して確かめるには `npm run deploy` が必要です。

## 集計する

Sink はデータを300秒ごとにまとめて書き出すので、リクエストを送ってからテーブルに反映されるまで数分かかります。

### CLI から

```sh
WRANGLER_BASIN_SQL_AUTH_TOKEN="$(cat .catalog_token)" \
npx wrangler r2 sql query <Warehouse 名> \
  "SELECT path, COUNT(*) AS hits FROM default.request_events WHERE path IS NOT NULL GROUP BY path ORDER BY hits DESC"
```

### ダッシュボードから

ダッシュボードの **R2 Data Catalog** で **Query data** を選ぶと、ブラウザ上の SQL エディタでクエリを実行できます。

## 注意点

- **Pipeline の SQL は変更できません。** 変えたいときは Pipeline を削除して作り直します。
- **Stream を json の1列にしないでください。** はじめは `value: json` の1列で作り、Pipeline の SQL で `json_get_str` や `->>` を使って列に展開しようとしましたが、どちらでも値がすべて NULL になりました。そのため、Stream を列ごとのスキーマ(`stream-schema.json`)にしています。
- **Pipelines 関連の API で `code: 1021` が出る場合**は、Workers Paid プランになっていません。プランを切り替えた直後は、反映されるまでしばらくこのエラーが出ることがあります。
