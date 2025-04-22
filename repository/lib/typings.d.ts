/**
 * LastAdCID --> [ ] -\
 * ↓ | ... | entries announced after we started the current walk ↓ | [ ] -/ ↓
 * head --> [ ] -\
 * ↓ | ... | entries visited in this walk ↓ | [ ] -/ ↓ tail --> [ ] -\
 * ↓ | ... | entries NOT visited yet ↓ | [ ] -/ ↓ lastHead --> [ ] -\
 * ↓ | ... | entries visited in the previous walks ↓ | [ ] -/ ↓ (null)
 */
export interface WalkerState {
  head?: string
  tail?: string
  lastHead?: string
  status: string
  entriesNotRetrievable?: number
  adsMissingPieceCID?: number
}

export type ProviderToWalkerStateMap = Map<string, WalkerState>

export type PiecePayloadCIDs = string[]
