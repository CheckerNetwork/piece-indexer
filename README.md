# piece-indexer

A lightweight IPNI node mapping Filecoin PieceCID → payload block CID.

- [Design doc](./docs/design.md)

## Basic use

Note: this will change soon, see https://github.com/filecoin-station/piece-indexer/issues/33

## `GET /sample/{provider-id}/{piece-cid}`

Sample a set of multihashes ingested by IPNI for a given ContextID, matching the route eventually to
be exposed by an IPNI reverse index.

_This is an alternative implementation of the IPNI Reverse Index as specified in
[xedni/openapi.yaml](https://github.com/ipni/xedni/blob/526f90f5a6001cb50b52e6376f8877163f8018af/openapi.yaml)._

Parameters:

- `provider-id` - the peer ID of the storage provider (index publisher)
- `piece-cid` - the Filecoin deal's PieceCID as advertised by the provider in Graphsync retrieval
  metadata

Response:

- `samples` - a list of exactly one payload block CID contained inside the piece identified by the
  requested PieceCID.

Example:

https://pix.filspark.com/sample/12D3KooWHKeaNCnYByQUMS2n5PAZ1KZ9xKXqsb4bhpxVJ6bBJg5V/baga6ea4seaqlwzed5tgjtyhrugjziutzthx2wrympvsuqhfngwdwqzvosuchmja

```json
{
  "samples": ["bafkreigrnnl64xuevvkhknbhrcqzbdvvmqnchp7ae2a4ulninsjoc5svoq"]
}
```

## `GET /ingestion-status/{provider-id}`

Return the index status for the given provider.

Example:

https://pix.filspark.com/ingestion-status/12D3KooWHKeaNCnYByQUMS2n5PAZ1KZ9xKXqsb4bhpxVJ6bBJg5V

```json
{
  "providerId": "12D3KooWHKeaNCnYByQUMS2n5PAZ1KZ9xKXqsb4bhpxVJ6bBJg5V",
  "ingestionStatus": "All advertisements from baguqeeralhduow57bhqo5zgiwe6swgjrqw2nuckvnch6nlylaensfzf3bfyq to the end of the chain were processed.",
  "lastHeadWalkedFrom": "baguqeeralhduow57bhqo5zgiwe6swgjrqw2nuckvnch6nlylaensfzf3bfyq",
  "adsMissingPieceCID": 0,
  "entriesNotRetrievable": 0,
  "piecesIndexed": 6344
}
```

## Development

```bash
docker run --name redis -p 6379:6379 -d redis
npm start -w indexer
```

## Deployment

Pushes to `main` will be deployed automatically.

Perform manual devops using [Fly.io](https://fly.io):

Indexer:

```bash
$ fly deploy --remote-only -c indexer/fly.toml
$ fly deploy --remote-only -c api/fly.toml
```
