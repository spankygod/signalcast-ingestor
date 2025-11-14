import { PolymarketMarket } from '../config/polymarket.js';

export interface NormalizedTick {
  market_polymarket_id: string;
  price: number;
  best_bid: number;
  best_ask: number;
  last_trade_price: number;
  liquidity: number;
  volume_24h: number;
  captured_at: string;
}

export function normalizeTick(market: PolymarketMarket): NormalizedTick {
  return {
    market_polymarket_id: market.id,
    price: market.currentPrice || 0,
    best_bid: market.bestBid || 0,
    best_ask: market.bestAsk || 0,
    last_trade_price: market.lastTradePrice || 0,
    liquidity: market.liquidity || 0,
    volume_24h: market.volume || 0,
    captured_at: new Date().toISOString()
  };
}
