export {
  PiecePayloadCIDs,
  ProviderToWalkerStateMap,
  WalkerState,
} from '@filecoin-station/spark-piece-indexer-repository/lib/typings.d.ts'

/* Response from `https://cid.contact/cid` */
export interface CidProviderEntry {
  AddrInfo: {
    ID: string
    Addrs: string[]
  }
  LastAdvertisement: {
    '/': string
  }
  LastAdvertisementTime: string
  Publisher: {
    ID: string
    Addrs: string[]
  }
  // Ignored: ExtendedProviders, FrozenAt
}

/** Data synced from IPNI */
export interface ProviderInfo {
  providerAddress: string
  lastAdvertisementCID: string
}

export type ProviderToInfoMap = Map<string, ProviderInfo>
