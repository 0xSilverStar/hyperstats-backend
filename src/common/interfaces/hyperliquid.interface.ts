export interface HyperLiquidCumFunding {
  allTime: string;
  sinceOpen: string;
  sinceChange: string;
}

export interface HyperLiquidPosition {
  coin: string;
  szi: string;
  leverage?: { type: string; value: number } | number;
  entryPx: string;
  positionValue: string;
  unrealizedPnl: string;
  returnOnEquity: string;
  liquidationPx?: string;
  marginUsed: string;
  maxLeverage?: number;
  cumFunding?: HyperLiquidCumFunding;
  markPx?: string;
}

export interface HyperLiquidAssetPosition {
  position: HyperLiquidPosition;
  type: string;
}

export interface HyperLiquidMarginSummary {
  accountValue: string;
  totalNtlPos: string;
  totalRawUsd: string;
  totalMarginUsed: string;
}

export interface HyperLiquidClearinghouseState {
  assetPositions: HyperLiquidAssetPosition[];
  crossMarginSummary?: HyperLiquidMarginSummary;
  marginSummary?: HyperLiquidMarginSummary;
  withdrawable?: string;
}

export interface HyperLiquidOrder {
  oid: number;
  coin: string;
  side: 'B' | 'A';
  orderType: string;
  limitPx: string;
  sz: string;
  reduceOnly: boolean;
  triggerCondition?: any;
  isPositionTpsl: boolean;
  cloid?: string;
  timestamp: number;
}

export interface HyperLiquidFill {
  hash: string;
  oid: number;
  coin: string;
  side: 'B' | 'A';
  px: string;
  sz: string;
  dir?: string;
  closedPnl?: string;
  fee?: string;
  feeToken?: string;
  startPosition?: string;
  crossed: boolean;
  time: number;
  tid?: number;
}

export interface HyperLiquidSpotBalance {
  coin: string;
  token: number;
  total: string;
  hold: string;
  entryNtl?: string;
}

export interface HyperLiquidSpotState {
  balances: HyperLiquidSpotBalance[];
}

export interface HyperLiquidMeta {
  universe: HyperLiquidAssetInfo[];
}

export interface HyperLiquidAssetInfo {
  name: string;
  szDecimals: number;
  maxLeverage: number;
  marginTableId?: number;
  onlyIsolated?: boolean;
  isDelisted?: boolean;
  marginMode?: string;
}

export interface HyperLiquidSpotMeta {
  tokens: HyperLiquidTokenInfo[];
}

export interface HyperLiquidTokenInfo {
  name: string;
  szDecimals?: number;
  weiDecimals?: number;
  index: number;
  tokenId: string;
}

export interface ArbiscanTransaction {
  hash: string;
  from: string;
  to: string;
  value: string;
  tokenDecimal: string;
  blockNumber: string;
  timeStamp: string;
}

export interface ArbiscanApiResponse {
  status: string;
  message: string;
  result: ArbiscanTransaction[] | string;
}
