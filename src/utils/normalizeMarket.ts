import { PolymarketEvent, PolymarketMarket, PolymarketOutcome } from '../config/polymarket.js';

export interface NormalizedEvent {
  polymarket_id: string;
  slug: string | null;
  title: string;
  description: string | null;
  category: string | null;
  subcategory: string | null;
  liquidity: number | null;
  volume: number | null;  // we reuse for both volume_24h & volume_total
  is_active: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  start_date: string | null;
  end_date: string | null;
}

export interface NormalizedMarket {
  polymarket_id: string;
  event_polymarket_id: string;
  question: string;
  slug: string;
  description: string;
  liquidity: number;
  volume: number;
  current_price: number;
  last_trade_price: number;
  best_bid: number;
  best_ask: number;
  status: string;
  resolved_at: string | null;
  is_active: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  approved: boolean;
  relevance_score: number;
}

export interface NormalizedOutcome {
  polymarket_id: string;
  market_polymarket_id: string;
  title: string;
  description: string;
  price: number;
  probability: number;
  volume: number;
  status: string;
  display_order: number;
}

export function normalizeEvent(event: PolymarketEvent): NormalizedEvent {
  return {
    polymarket_id: event.id,
    slug: event.slug || null,
    title: event.title,
    description: event.description || null,
    category: event.category || null,
    subcategory: null, // not used yet
    liquidity: event.liquidity || null,
    volume: event.volume || null,
    is_active: event.isActive,
    closed: event.closed,
    archived: event.archived,
    restricted: event.restricted,
    start_date: event.startDate || null,
    end_date: event.endDate || null
  };
}

export function normalizeMarket(
  market: PolymarketMarket,
  event: Pick<PolymarketEvent, 'id'>
): NormalizedMarket {
  const liquidityScore = Math.log10(Math.max(market.liquidity || 1, 1));
  const volumeScore = Math.log10(Math.max(market.volume || 1, 1));
  const relevance = Math.min(Math.round(liquidityScore + volumeScore) + (market.isActive ? 2 : 0), 10);

  return {
    polymarket_id: market.id,
    event_polymarket_id: event.id,
    question: market.question,
    slug: market.slug,
    description: market.description || '',
    liquidity: market.liquidity || 0,
    volume: market.volume || 0,
    current_price: market.currentPrice || 0,
    last_trade_price: market.lastTradePrice || 0,
    best_bid: market.bestBid || 0,
    best_ask: market.bestAsk || 0,
    status: mapMarketStatus(market.status, market.closed),
    resolved_at: market.resolvedAt || null,
    is_active: market.isActive,
    closed: market.closed,
    archived: market.archived,
    restricted: market.restricted,
    approved: market.approved,
    relevance_score: relevance
  };
}

export function normalizeOutcome(outcome: PolymarketOutcome, market: PolymarketMarket): NormalizedOutcome {
  return {
    polymarket_id: outcome.id,
    market_polymarket_id: market.id,
    title: outcome.title,
    description: outcome.description || '',
    price: outcome.price,
    probability: outcome.probability,
    volume: outcome.volume,
    status: mapOutcomeStatus(outcome.status),
    display_order: outcome.displayOrder
  };
}

function mapMarketStatus(status: string, closed: boolean): string {
  if (closed) return 'resolved';
  if (status === 'paused') return 'paused';
  if (status === 'cancelled') return 'cancelled';
  return 'active';
}

function mapOutcomeStatus(status: string): string {
  switch (status) {
    case 'resolved_true':
    case 'resolved_false':
      return status;
    case 'cancelled':
      return 'cancelled';
    default:
      return 'active';
  }
}
