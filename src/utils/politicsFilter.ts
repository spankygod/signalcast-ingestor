import { PolymarketEvent, PolymarketMarket } from '../config/polymarket';
import { POLITICS_KEYWORDS } from './constants';

export class PoliticsFilter {
  filterEvents(events: PolymarketEvent[]): PolymarketEvent[] {
    return events.filter(event => this.isPoliticsEvent(event));
  }

  filterMarkets(markets: PolymarketMarket[]): PolymarketMarket[] {
    return markets.filter(market => this.isPoliticsMarket(market));
  }

  private isPoliticsEvent(event: PolymarketEvent): boolean {
    return this.containsPoliticsKeywords(event.title) || 
           this.containsPoliticsKeywords(event.description || '');
  }

  private isPoliticsMarket(market: PolymarketMarket): boolean {
    return this.containsPoliticsKeywords(market.question) || 
           this.containsPoliticsKeywords(market.description || '');
  }

  private containsPoliticsKeywords(text: string): boolean {
    const lowerText = text.toLowerCase();
    
    // Check all keyword arrays
    const allKeywords = [
      ...POLITICS_KEYWORDS.slug,
      ...POLITICS_KEYWORDS.title,
      ...POLITICS_KEYWORDS.category
    ];
    
    // Check for exclude keywords first
    const hasExcluded = POLITICS_KEYWORDS.exclude.some(keyword => 
      lowerText.includes(keyword.toLowerCase())
    );
    
    if (hasExcluded) return false;
    
    // Check for politics keywords
    return allKeywords.some(keyword => 
      lowerText.includes(keyword.toLowerCase())
    );
  }
}

export const politicsFilter = new PoliticsFilter();
