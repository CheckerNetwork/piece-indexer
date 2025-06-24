export type CidRetrievalProvider = {
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

export type IpniCidQueryResponse = CidRetrievalProvider[]
