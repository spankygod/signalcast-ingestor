export const QUEUES = {
  events: 'signalcast:queue:events',
  markets: 'signalcast:queue:markets',
  outcomes: 'signalcast:queue:outcomes',
  ticks: 'signalcast:queue:ticks'
} as const;

export const WORKERS = {
  marketChannel: 'wss-market-channel',
  userChannel: 'wss-user-channel',
  marketsPoller: 'markets-poller',
  outcomesPoller: 'outcomes-poller',
  eventsPoller: 'events-poller',
  dbWriter: 'db-writer',
  heartbeat: 'heartbeat'
} as const;

export const POLITICS_KEYWORDS = {
  slug: [
    'politics',
    'trump',
    'biden',
    'election',
    'senate',
    'president',
    'white-house'
  ],
  title: [
    'election',
    'president',
    'congress',
    'senate',
    'trump',
    'biden'
  ],
  category: [
    'politics',
    'government',
    'elections',
    'us-politics',
    'world-politics',
    'us election',
    'us politics',
    'trump presidency',
    'geopolitics',
    'world',
    'middle east',
    'economy',
    'finance'
  ],
  exclude: [
    'nba',
    'nfl',
    'soccer',
    'ufc',
    'golf'
  ]
} as const;
