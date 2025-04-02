export {
  PiecePayloadCIDs,
  ProviderToWalkerStateMap,
  WalkerState,
} from '@filecoin-station/spark-piece-indexer-repository/lib/typings.d.ts'

/**
 * Data synced from IPNI
 */
export interface ProviderInfo {
  providerAddress: string
  lastAdvertisementCID: string
}

export type ProviderToInfoMap = Map<string, ProviderInfo>
