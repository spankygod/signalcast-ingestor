import { PolymarketMarket } from '../config/polymarket.js';

export interface NormalizedTick {
  market_polymarket_id: string;
  price: number | null;
  best_bid: number | null;
  best_ask: number | null;
  last_trade_price: number | null;
  liquidity: number | null;
  volume_24h: number | null;
  captured_at: Date;
}

export function normalizeTick(market: PolymarketMarket): NormalizedTick {
  return {
    market_polymarket_id: market.id,
    price: market.currentPrice,
    best_bid: market.bestBid,
    best_ask: market.bestAsk,
    last_trade_price: market.lastTradePrice,
    liquidity: market.liquidity ?? null,
    volume_24h: market.volume ?? null,
    captured_at: new Date()
  };
}
