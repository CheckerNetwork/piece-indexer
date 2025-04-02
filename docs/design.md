# Design Doc: Piece Indexer

## Introduction

### Context

Filecoin defines a `Piece` as the main unit of negotiation for data that users _store_ on the
Filecoin network. This is reflected in the on-chain metadata field `PieceCID`.

On the other hand, content is _retrieved_ from Filecoin using the CID of the requested payload.

This dichotomy poses a challenge for retrieval checkers like Spark: for a given deal storing some
PieceCID, what payload CID to request when testing the retrieval?

Spark v1 relies on StorageMarket's DealProposal metadata `Label`, which is often (but not always!)
set by the client to the root CID of the payload stored.

In the first half of 2024, the Filecoin network added support for DDO (Direct Data Onboarding)
deals. These deals don't have any DealProposal, there is no `Label` field. Only `PieceCID`. We need
to find a different solution to support these deals.

In the longer term, we want IPNI to provide a reverse lookup functionality that will allow clients
like Spark to request a sample of payload block CIDs associated with the given ContextID or
PieceCID, see the
[design proposal for InterPlanetary Piece Index](https://docs.google.com/document/d/1jhvP48ccUltmCr4xmquTnbwfTSD7LbO1i1OVil04T2w).

To cover the gap until the InterPlanetary Piece Index is live, we want to implement a lightweight
solution that can serve as a stepping stone between Label-based CID discovery and full Piece Index
sampling.

### High-Level Idea

Let's implement a lightweight IPNI ingester that will process the advertisements from Filecoin SPs
and extract the list of `(ProviderID, PieceCID, PayloadCID)` entries. Store these entries in a
persisted datastore (Redis). Provide a REST API endpoint accepting `(ProviderID, PieceCID)` and
returning a single `PayloadCID`.

### Terminology

- **Indexer:** A network node that keeps a mappings of multihashes to provider records. Example:
  https://cid.contact

- **Index Provider (a.k.a Publisher):** An entity that publishes advertisements and index data to an
  indexer. It is usually, but not always, the same as the data provider. Example: a
  [Boost](https://boost.filecoin.io) instance operated by a Storage Provider.

Quoting from
[IPNI spec](https://github.com/ipni/specs/blob/90648bca4749ef912b2d18f221514bc26b5bef0a/IPNI.md#terminology):

- **Advertisement**: A record available from a publisher that contains, a link to a chain of
  multihash blocks, the CID of the previous advertisement, and provider-specific content metadata
  that is referenced by all the multihashes in the linked multihash blocks. The provider data is
  identified by a key called a context ID.

- **Announce Message**: A message that informs indexers about the availability of an advertisement.
  This is usually sent via gossip pubsub, but can also be sent via HTTP. An announce message
  contains the advertisement CID it is announcing, which allows indexers to ignore the announce if
  they have already indexed the advertisement. The publisher's address is included in the announce
  to tell indexers where to retrieve the advertisement from.

- **Context ID**: A key that, for a provider, uniquely identifies content metadata. This allows
  content metadata to be updated or delete on the indexer without having to refer to it using the
  multihashes that map to it.

- **Metadata**: Provider-specific data that a retrieval client gets from an indexer query and passed
  to the provider when retrieving content. This metadata is used by the provider to identify and
  find specific content and deliver that content via the protocol (e.g. graphsync) specified in the
  metadata.

- **Provider**: Also called a Storage Provider, this is the entity from which content can be
  retrieved by a retrieval client. When multihashes are looked up on an indexer, the responses
  contain provider that provide the content referenced by the multihashes. A provider is identified
  by a libp2p peer ID.

- **Publisher**: This is an entity that publishes advertisements and index data to an indexer. It is
  usually, but not always, the same as the data provider. A publisher is identified by a libp2p peer
  ID.

### Notes

**System Components**

There are two components in this design:

- A deal tracker component observing StorageMarket & DDO deals to build a list of deals eligible for
  Spark retrieval testing. We define deal as a tuple `(PieceCID, MinerID, ClientID)`.

- A piece indexer observing IPNI announcements to build an index mapping PieceCIDs to PayloadCIDs.

When Spark builds a list of tasks for the current round, it will ask the deal tracker for 1000
active deals. This ensures we test retrievals for active deals only.

When Spark checker tests retrieval, it will first consult the piece indexer to convert deal's
PieceCID to a payload CID to retrieve.

**Expired Deals**

Pieces are immutable. If we receive an advertisement saying that a payload block CID was found in a
piece CID, then this information remains valid forever, even after the SP advertise that they are no
longer storing that block. This means our indexer can ignore `IsRm` advertisements.

It's ok if the piece indexer stores data for expired deals, because Spark is not going to ask for
that data. (Of course, there is the cost of storing data we don't need, but we don't have to deal
with that yet.)

**Payload CIDs Are Scoped to Providers**

The indexer protocol does not provide any guarantees about the list of CIDs advertised for the same
Piece CID. Different SPs can advertise different lists (e.g. the entries can be ordered differently)
or can even cheat and submit CIDs that are not part of the Piece. Our indexer must scope the
information to each index provider (each Filecoin SP).

### Anatomy of IPNI Advertisements

Quoting from
[Ingestion](https://github.com/ipni/specs/blob/90648bca4749ef912b2d18f221514bc26b5bef0a/IPNI.md#ingestion):

> The indexer reads the advertisement chain starting from the head, reading previous advertisements
> until a previously seen advertisement, or the end of the chain, is reached. The advertisements and
> their entries are then processed in order from earliest to head.

An Advertisement has several properties (see
[the full spec](https://github.com/ipni/specs/blob/90648bca4749ef912b2d18f221514bc26b5bef0a/IPNI.md#advertisements)),
we need the following ones:

- **`PreviousID`** is the CID of the previous advertisement, and is empty for the 'genesis'.

- **`Metadata`** represents additional opaque data. The metadata for Graphsync retrievals includes
  the PieceCID that we are looking for.

- **`Entries`** is a link to a data structure that contains the advertised multihashes. For our
  purposes, it's enough to take the first entry and ignore the rest.

Advertisements are made available for consumption by indexer nodes as a set of files that can be
fetched via HTTP.

Quoting from
[Advertisement Transfer](https://github.com/ipni/specs/blob/90648bca4749ef912b2d18f221514bc26b5bef0a/IPNI.md#advertisement-transfer):

> All IPNI HTTP requests use the IPNI URL path prefix, `/ipni/v1/ad/`. Indexers and advertisement
> publishers implicitly use and expect this prefix to precede the requested resource.
>
> The IPLD objects of advertisements and entries are represented as files named by their CIDs in an
> HTTP directory. These files are immutable, so can be safely cached or stored on CDNs. To fetch an
> advertisement or entries file by CID, the request made by the indexer to the publisher is
> `GET /ipni/v1/ad/{CID}`.

The IPNI instance running at https://cid.contact provides an API returning the list of all index
providers from which cid.contact have received announcements:

https://cid.contact/providers

The response provides all the metadata we need to download the advertisements. For each index
provider, the response includes:

- `Publisher.Addrs` describes where we can contact SP's index provider to retrieve content for CIDs,
  e.g., advertisements.
- `LastAdvertisement` contains the CID of the head advertisement from the SP

## Proposed Design

### Ingestion

Ingesting announcements from all Storage Providers is the most complex component. For each storage
provider, we need to periodically check for the latest advertisement head and process the chain from
head until we find an advertisement we have already processed before. The chain can be very long,
therefore we need to account for the cases when the service restarts or a new head is published
before we finish processing the chain.

#### Proposed algorithm

**Per-provider state**

Use the following per-provider state:

- `providerId` - Primary key.

- Provider info obtained from IPNI - stored in memory only:

  - `providerAddress` - Provider's address where we can fetch advertisements from.

  - `lastAdvertisementCID` - The CID of the most recent head seen by cid.contact. This is where we
    need to start the next walk from.

- Provider walker state - persisted in our datastore (Redis):

  - `head` - The CID of the head advertisement we started the current walk from. We update this
    value whenever we start a new walk.

  - `tail` - The CID of the next advertisement in the chain that we need to process in the current
    walk.

  - `lastHead` - The CID of the head where we started the previous walk (the last walk that has
    already finished). All advertisements from `lastHead` to the end of the chain have already been
    processed.

    > **Note:** The initial walk will take a long time to complete. While we are walking the "old"
    > chain, new advertisements (new heads) will be announced to IPNI.
    >
    > - `lastAdvertisementCID` is the latest head announced to IPNI
    > - `head` is the advertisement where the current walk-in-progress started
    >
    > I suppose we don't need to keep track of `lastAdvertisementCID`. When the current walk
    > finishes, we could wait up to one minute until we make another request to cid.contact to find
    > what are the latest heads for each SPs.
    >
    > In the current proposal, when the current walk finishes, we can immediately continue with
    > walking from the `lastAdvertisementCID`.

We must always walk the chain all the way to the genesis or to the entry we have already seen &
processed.

The current walk starts from `head` and walks up to `lastHead`. When the current walk reaches
`lastHead`, we need to set `lastHead ← head` so that the next walk knows where to stop.

`lastAdvertisementCID` is updated every minute when we query cid.contact for the latest heads. If
the walk takes longer than a minute to finish, then `lastAdvertisementCID` will change and we cannot
use it for `lastHead`.

Here is how the state looks like in the middle of a walk:

```
lastAdCID --> [ ] -\
               ↓    |
              ...   | entries announced after we started the current walk
               ↓    |
              [ ] -/
               ↓
     head --> [ ] -\
               ↓    |
              ...   | entries visited in this walk
               ↓    |
              [ ] -/
               ↓
     tail --> [ ] -\
               ↓    |
              ...   | entries NOT visited yet
               ↓    |
              [ ] -/
               ↓
 lastHead --> [ ] -\
               ↓    |
              ...   | entries visited in the previous walks
               ↓    |
              [ ] -/
               ↓
             (null)
```

**Track latest advertisements**

Every minute, run the following high-level loop:

1. Fetch the list of providers and their latest advertisements (heads) from
   https://cid.contact/providers. (This is **one** HTTP request.)

2. Update the in-memory info we keep for each provider (address, CID of the last advertisement).

> **Note:** Instead of running the loop every minute, we can introduce a one-minute delay between
> the iterations instead. It should not matter too much in practice, though. I expect each iteration
> to finish within one minute, as it's just a single HTTP call to cid.contact.

**Walk advertisement chains (in background)**

The chain-walking algorithm runs in the background and loops over the following steps.

1. Preparation

   - If `tail` is not null, then there is an ongoing walk of the chain we need to continue.

   - Otherwise, if `nextHead` is the same as `lastHead`, then there are no new advertisement to
     process and the walk immediately returns.

   - Otherwise, we are starting a new walk. Update the walker state as follows:

     ```
     head := newHead
     tail := newHead
     ```

     (`lastHead` does not change until we finish the walk.)

2. Take one step

   1. Fetch the advertisement identified by `tail` from the index provider.

   2. Process the metadata and entries to extract up to one `(PieceCID, PayloadCID)` entry to be
      added to the index and `PreviousID` linking to the next advertisement in the chain to process.

3. Update the worker state

   - If `PreviousID == lastHead || PreviousID == null`, then we finished the walk. Update the state
     as follows:

     ```
     lastHead := head
     head := null
     tail := null
     ```

   - Otherwise, update the `tail` field using the `PreviousID` field from the advertisement.

     ```
     tail := PreviousID
     ```

4. Persist the new state in the database.

#### Handling the Scale

At the time of writing this document, cid.contact was tracking 322 index providers. From Sparks'
measurements, we know there are an additional 843 storage providers that don't advertise to IPNI.
The number of storage providers grows over time, our system must be prepared to ingest
advertisements from thousands of providers.

Each storage/index provider produces tens of thousands to hundreds of thousands of advertisements
(in total). The initial ingestion run will take a while to complete. We also must be careful to not
overload the SP by sending too many requests.

The design outlined in the previous section divides the ingestion process into small steps that can
be scheduled and executed independently. This allows us to avoid the complexity of managing
long-running per-provider tasks and instead repeatedly execute one step of the process.

However, requests for advertisements from providers can take different amount of time. Some
providers are not configured properly and the request fails after a long timeout. Such slow
providers must not block the ingestion of advertisements from faster providers, therefore we still
need some sort of a per-provider task runner.

### REST API

Implement the following endpoint that will be called by Spark checker nodes. The endpoint will sign
the response using the server's private key to allow spark-evaluate to verify the authenticity of
results reported by checker nodes:

```
GET /sample/{providerId}/{pieceCid}?seed={seed}
```

Response in JSON format, when the piece was found:

```json
{
  "samples": ["exactly one CID of a payload block advertised for PieceCID"],
  "pubkey": "server's public key",
  "signature": "signature over dag-json{providerId,pieceCid,seed,samples}"
}
```

Response in JSON format, when the piece or the provider was not found:

```json
{
  "error": "code - e.g. PROVIDER_NOT_FOUND or PIECE_NOT_FOUND",
  "pubkey": "server's public key",
  "signature": "signature over dag-json{providerId,pieceCid,seed,error}"
}
```

In the initial version, the server will ignore the `seed` value and use it only as a nonce
preventing replay attacks. Spark checker nodes will set the seed using the DRAND randomness string
for the current Spark round.

In the future, when IPNI implements the proposed reverse-index sampling endpoint, the seed will be
used to pick the samples at random. See the
[IPNI Multihash Sampling API proposal](https://github.com/ipni/xedni/blob/526f90f5a6001cb50b52e6376f8877163f8018af/openapi.yaml)

### Observability

We need visibility into the status of ingestion for any given provider. Some providers don't
advertise at all, some may have misconfigured integration with IPNI, we need to understand why our
index does not include any data for a provider.

Let's enhance the state table with another column describing the ingestion status as a free-form
string and implement a new REST API endpoint to query the ingestion status.

```
GET /ingestion-status/{providerId}
```

Response in JSON format:

```json
{
  "providerId": "state.providerId",
  "providerAddress": "state.providerAddress",
  "ingestionStatus": "state.ingestion_status",
  "lastHeadWalkedFrom": "state.lastHead",
  "piecesIndexed": 123
  // ^^ number of (PieceCID, PayloadCID) records found for this provider
}
```
